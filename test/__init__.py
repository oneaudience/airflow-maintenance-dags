from datetime import datetime


class DateTimeRange:
    """
    Class representing a time range. If compared to a datetime object,
    it will check whether the time falls in the range represented by this object.
    """

    def __init__(self, start, end):
        """
        Define an inclusive time range between `start` and `end`

        :param start: Start of time range
        :type start: datetime
        :param end: End of time range
        :type end: datetime
        """
        self.start = start
        self.end = end

    @classmethod
    def from_task_instance(cls, ti):
        """Create a DateTimeRange based on a TaskInstance's start_date and end_date

        :param ti: The task instance to use
        :type ti: airflow.models.TaskInstance
        :rtype DateTimeRange:
        """
        return DateTimeRange(
            start=ti.start_date.replace(tzinfo=None),
            end=ti.end_date.replace(tzinfo=None)
        )

    def __eq__(self, other):
        if isinstance(other, DateTimeRange):
            return self.start == other.start and self.end == other.end
        elif isinstance(other, datetime):
            return self.start <= other <= self.end
        return False

    def __str__(self):
        return f'[{self.start} - {self.end}]'

    def __repr__(self):
        return f'<{self.__class__.__name__} {self}>'
