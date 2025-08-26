from django.db import models

class Agency(models.Model):
    agency_id = models.CharField(max_length=50, primary_key=True)
    name = models.CharField(max_length=255)
    url = models.URLField()
    timezone = models.CharField(max_length=50)
    lang = models.CharField(max_length=10, blank=True, null=True)

    def __str__(self):
        return self.name


class Stop(models.Model):
    stop_id = models.CharField(max_length=50, primary_key=True)
    name = models.CharField(max_length=255)
    lat = models.FloatField()
    lon = models.FloatField()
    wheelchair_boarding = models.BooleanField(default=False)

    def __str__(self):
        return self.name
