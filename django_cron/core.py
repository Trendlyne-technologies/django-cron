import logging
from datetime import timedelta
import traceback
import time
import sys
import pytz

from django.conf import settings
from django.utils.timezone import now as utc_now
from django.db.models import Q
from subprocess import check_output
from django_cron.helpers import get_class, get_current_time

from django.core.mail import send_mail
import os
import psutil


DEFAULT_LOCK_BACKEND = 'django_cron.backends.lock.cache.CacheLock'
DJANGO_CRON_OUTPUT_ERRORS = False
logger = logging.getLogger('django_cron')


class BadCronJobError(AssertionError):
    pass


class Schedule(object):
    def __init__(
            self,
            run_every_mins=None,
            run_at_times=None,
            retry_after_failure_mins=None,
            run_weekly_on_days=None,
            run_monthly_on_days=None,
            run_tolerance_seconds=0,
            run_between_times=None,
            tz=None
    ):
        if run_at_times is None:
            run_at_times = []
        if run_between_times is None:
            run_between_times = []
        self.run_every_mins = run_every_mins
        self.run_at_times = run_at_times
        self.retry_after_failure_mins = retry_after_failure_mins
        self.run_weekly_on_days = run_weekly_on_days
        self.run_monthly_on_days = run_monthly_on_days
        self.run_tolerance_seconds = run_tolerance_seconds
        self.run_between_times = run_between_times
        # tz = "Pacific/Johnston" if left none setting.tz will be used else utc timezone will be used
        if tz is None:
            tz = "Asia/Kolkata"
        self.tz = tz


class CronJobBase(object):
    """
    Sub-classes should have the following properties:
    + code - This should be a code specific to the cron being run. Eg. 'general.stats' etc.
    + schedule

    Following functions:
    + do - This is the actual business logic to be run at the given schedule
    """
    SEND_FAILED_EMAIL = []
    remove_successful_cron_logs = False

    def __init__(self):
        self.prev_success_cron = None

    @classmethod
    def get_code(self):
        try:
            if self.APPEND_IP_TO_CODE:
                myserverip = None
                try:
                    myserverip = check_output(['/usr/bin/ec2metadata', '--public-ipv4'])
                    myserverip = myserverip.strip()
                except:
                    pass
                if not myserverip:
                    myserverip = check_output(['hostname', '-I'])
                    myserverip = myserverip.strip()
                return self.code + '-' + myserverip.decode('utf-8')
        except:
            return self.code

    def set_prev_success_cron(self, prev_success_cron):
        self.prev_success_cron = prev_success_cron

    def get_prev_success_cron(self):
        return self.prev_success_cron

    @classmethod
    def get_time_until_run(cls):
        from django_cron.models import CronJobLog

        try:
            last_job = CronJobLog.objects.filter(code=cls.code).latest('start_time')
        except CronJobLog.DoesNotExist:
            return timedelta()
        return (
                last_job.start_time
                + timedelta(minutes=cls.schedule.run_every_mins)
                - utc_now()
        )


class CronJobManager(object):
    """
    A manager instance should be created per cron job to be run.
    Does all the logger tracking etc. for it.
    Used as a context manager via 'with' statement to ensure
    proper logger in cases of job failure.
    """

    def __init__(self, cron_job_class, silent=False, dry_run=False, stdout=None):
        self.cron_job_class = cron_job_class
        self.silent = silent
        self.dry_run = dry_run
        self.stdout = stdout or sys.stdout
        self.lock_class = self.get_lock_class()
        self.previously_ran_successful_cron = None
        self.write_log = getattr(
            settings, 'DJANGO_CRON_OUTPUT_ERRORS', DJANGO_CRON_OUTPUT_ERRORS
        )
        self.send_error_email = getattr(settings, 
                                        'DJANGO_CRON_SEND_IMMEDIATE_ERROR_EMAIL', 
                                        False)

    def should_run_now(self, force=False):
        from django_cron.models import CronJobLog

        cron_job = self.cron_job
        """
        Returns a boolean determining whether this cron should run now or not!
        """
        self.user_time = None
        self.previously_ran_successful_cron = None

        # If we pass --force options, we force cron run
        if force:
            return True

        if cron_job.schedule.run_monthly_on_days is not None:
            if not get_current_time(cron_job.schedule.tz).day in cron_job.schedule.run_monthly_on_days:
                return False

        if cron_job.schedule.run_weekly_on_days is not None:
            if not get_current_time(cron_job.schedule.tz).weekday() in cron_job.schedule.run_weekly_on_days:
                return False

        if cron_job.schedule.retry_after_failure_mins:
            # We check last job - success or not
            last_job = (
                CronJobLog.objects.filter(code=cron_job.get_code())
                    .order_by('-start_time')
                    .exclude(start_time__gt=get_current_time(cron_job.schedule.tz))
                    .first()
            )
            if (
                    last_job
                    and not last_job.is_success
                    and get_current_time(cron_job.schedule.tz) + timedelta(seconds=cron_job.schedule.run_tolerance_seconds)
                    <= last_job.start_time.astimezone(pytz.timezone(cron_job.schedule.tz))
                    + timedelta(minutes=cron_job.schedule.retry_after_failure_mins)
            ):
                return False

        if cron_job.schedule.run_every_mins is not None:
            # two cases to handle if run_between_times exist and normal flow of run_every_mins
            now = get_current_time(cron_job.schedule.tz)
            actual_time = time.strptime("%s:%s" % (now.hour, now.minute), "%H:%M")
            check = False
            interval = None
            if cron_job.schedule.run_between_times:
                # running loop over run_between_times to check if now exist in between any interval
                for i in cron_job.schedule.run_between_times:
                    if actual_time >= time.strptime(i[0], "%H:%M") and actual_time <= time.strptime(i[1], "%H:%M"):
                        interval = i
                        check = True
            else:
                check = True
            # check is False cron will not run
            if not check:
                return False
            try:
                self.previously_ran_successful_cron = CronJobLog.objects.filter(
                    code=cron_job.get_code(),
                    is_success=True,
                    ran_at_time__isnull=True
                ).latest('start_time')
            except CronJobLog.DoesNotExist:
                pass

            if self.previously_ran_successful_cron:
                start_time = self.previously_ran_successful_cron.start_time.astimezone(pytz.timezone(cron_job.schedule.tz))
                # there is breakage in between intervals ,so to run it in interval replacing start time with now
                if interval and actual_time == time.strptime(interval[0], "%H:%M") and now.replace(second=0,microsecond=0) != start_time.replace(second=0,microsecond=0):
                    start_time = now
                else:
                    start_time += timedelta(minutes=cron_job.schedule.run_every_mins)
                if get_current_time(cron_job.schedule.tz) > start_time :
                    return True
            else:
                return True

        if cron_job.schedule.run_at_times:
            for time_data in cron_job.schedule.run_at_times:
                user_time = time.strptime(time_data, "%H:%M")
                now = get_current_time(cron_job.schedule.tz)
                actual_time = time.strptime("%s:%s" % (now.hour, now.minute), "%H:%M")
                if actual_time >= user_time:
                    qset = CronJobLog.objects.filter(
                        code=cron_job.get_code(), ran_at_time=time_data, is_success=True
                    ).filter(
                        Q(start_time__gt=now)
                        | Q(
                            end_time__gte=now.replace(
                                hour=0, minute=0, second=0, microsecond=0
                            )
                        )
                    )
                    if not qset:
                        self.user_time = time_data
                        return True

        return False

    def make_log(self, *messages, **kwargs):
        cron_log = self.cron_log

        cron_job = getattr(self, 'cron_job', self.cron_job_class)
        cron_log.code = cron_job.get_code()

        cron_log.is_success = kwargs.get('success', True)
        cron_log.message = self.make_log_msg(messages)
        cron_log.ran_at_time = getattr(self, 'user_time', None)
        cron_log.end_time = get_current_time(cron_job.schedule.tz)
        cron_log.save()

        if not cron_log.is_success and self.write_log:
            logger.error("%s cronjob error:\n%s" % (cron_log.code, cron_log.message))
        
        if self.send_error_email and not cron_log.is_success:
            from django_cron.models import CronJobLog
            try:
                emails = [admin[1] for admin in settings.ADMINS]
                if getattr(cron_job, "SEND_FAILED_EMAIL", []):
                    emails.extend(cron_job.SEND_FAILED_EMAIL)
                
                failed_runs_cronjob_email_prefix = getattr(settings, 'FAILED_RUNS_CRONJOB_EMAIL_PREFIX', '')
                min_failures = getattr(cron_job, 'MIN_NUM_FAILURES', 10)
                if not min_failures:
                    min_failures = 10

                last_min_cron_status = list(CronJobLog.objects.using("default").filter(
                        code=cron_log.code).order_by("-end_time").values_list("is_success", flat=True)[:min_failures])

                #All of them should be failed ie false. Then only we have to send email
                # Send on 3 failures. [True, False, False] ie [success, failed, failed] does not trigger email
                if not any(last_min_cron_status):
                    send_mail(
                        '%s%s failed %s times in a row!' % (
                            failed_runs_cronjob_email_prefix,
                            cron_log.code,
                            min_failures,
                        ),
                        cron_log.message,
                        settings.DEFAULT_FROM_EMAIL, emails
                    )
            except Exception as e:
                logger.exception(e)

    def make_log_msg(self, messages):
        full_message = ''
        if messages:
            for message in messages:
                if len(message):
                    full_message += message
                    full_message += '\n'

        return full_message

    def __enter__(self):
        cron_job = getattr(self, 'cron_job', self.cron_job_class)
        from django_cron.models import CronJobLog

        self.cron_log = CronJobLog(start_time=get_current_time(cron_job.schedule.tz))

        return self

    def __exit__(self, ex_type, ex_value, ex_traceback):
        if ex_type is None:
            return True

        non_logging_exceptions = [BadCronJobError, self.lock_class.LockFailedException]

        if ex_type in non_logging_exceptions:
            if not self.silent:
                self.stdout.write("{0}\n".format(ex_value))
                logger.info(ex_value)
        else:
            if not self.silent:
                self.stdout.write(
                    u"[\N{HEAVY BALLOT X}] {0}\n".format(self.cron_job_class.code)
                )
            try:
                trace = "".join(
                    traceback.format_exception(ex_type, ex_value, ex_traceback)
                )
                self.make_log(self.msg, trace, success=False)
            except Exception as e:
                err_msg = "Error saving cronjob (%s) log message: %s" % (
                    self.cron_job_class,
                    e,
                )
                logger.error(err_msg)

        return True  # prevent exception propagation

    def run(self, force=False):
        """
        apply the logic of the schedule and call do() on the CronJobBase class
        """
        cron_job_class = self.cron_job_class

        if not issubclass(cron_job_class, CronJobBase):
            raise BadCronJobError(
                'The cron_job to be run must be a subclass of %s' % CronJobBase.__name__
            )

        if not hasattr(cron_job_class, 'code'):
            raise BadCronJobError(
                "Cron class '{0}' does not have a code attribute".format(
                    cron_job_class.__name__
                )
            )

        with self.lock_class(cron_job_class, self.silent):
            self.cron_job = cron_job_class()

            if self.should_run_now(force):
                if not self.dry_run:
                    logger.debug(
                        "Running cron: %s code %s",
                        cron_job_class.__name__,
                        self.cron_job.get_code(),
                    )
                    self.make_log(f'Job in progress {os.getpid()}', success=True)
                    self.msg = self.cron_job.do()
                    self.make_log(self.msg, success=True)
                    self.cron_job.set_prev_success_cron(
                        self.previously_ran_successful_cron
                    )
                if not self.silent:
                    self.stdout.write(
                        u"[\N{HEAVY CHECK MARK}] {0}\n".format(self.cron_job.code)
                    )
                self._remove_old_success_job_logs(cron_job_class)
            elif not self.silent:
                self.stdout.write(u"[ ] {0}\n".format(self.cron_job.code))

    def get_lock_class(self):
        name = getattr(settings, 'DJANGO_CRON_LOCK_BACKEND', DEFAULT_LOCK_BACKEND)
        try:
            return get_class(name)
        except Exception as err:
            raise Exception("invalid lock module %s. Can't use it: %s." % (name, err))

    @property
    def msg(self):
        return getattr(self, '_msg', '')

    @msg.setter
    def msg(self, msg):
        if msg is None:
            msg = ''
        
        try:
            pid = os.getpid()
            process = psutil.Process(pid)
            memory_usage = process.memory_info().rss
            memory_usage_mb = memory_usage / (1024 * 1024)
            msg = "PID: {}, Memory MB: {} \n\n {}".format(os.getpid(), memory_usage_mb, msg)
        except Exception as e:
            pass
        
        self._msg = msg

    def _remove_old_success_job_logs(self, job_class):
        if job_class.remove_successful_cron_logs or getattr(settings, 'REMOVE_SUCCESSFUL_CRON_LOGS', False):
            from django_cron.models import CronJobLog
            CronJobLog.objects.filter(code=job_class.code, is_success=True).exclude(pk=self.cron_log.pk).delete()
