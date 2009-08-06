from datetime import datetime, timedelta
import feedparser
from urllib import urlencode
import urllib2

from django.conf import settings
from django.contrib.sites.models import Site
from django.core.urlresolvers import reverse, Resolver404
from django.db import models
from django.utils.hashcompat import sha_constructor

DEFAULT_LEASE_SECONDS = 2592000 # 30 days in seconds

class SubscriptionManager(models.Manager):

    def subscribe(self, topic, hub=None, callback=None,
                  lease_seconds=None):
        if hub is None:
            hub = self._get_hub(topic)

        if hub is None:
            raise TypeError(
                'hub cannot be None if the feed does not provide it')

        if lease_seconds is None:
            lease_seconds = getattr(settings, 'PUBSUBHUBBUB_LEASE_SECONDS',
                                   DEFAULT_LEASE_SECONDS)

        subscription, created = self.get_or_create(
            hub=hub, topic=topic)
        subscription.set_expiration(lease_seconds)

        if callback is None:
            try:
                callback_path = reverse('pubsubhubbub_callback',
                                        args=(subscription.pk,))
            except Resolver404:
                raise TypeError(
                    'callback cannot be None if there is not a reverable URL')
            else:
                callback = 'http://' + Site.objects.get_current() + \
                    callback_path

        response = self._send_request(hub, {
                'mode': 'subscribe',
                'callback': callback,
                'topic': topic,
                'verify': ('async', 'sync'),
                'verify_token': subscription.generate_token('subscribe'),
                'lease_seconds': lease_seconds,
                })

        info = response.info()
        if info.status == 204:
            subscription.verified = True
        elif info.status == 202: # async verification
            subscription.verified = False
        else:
            error = response.read()
            raise urllib2.URLError('error subscribing to %s on %s:\n%s' % (
                    topic, hub, error))

        subscription.save()
        return subscription


    def _get_hub(self, topic):
        parsed = feedparser.parse(topic)
        for link in parsed.feed.links:
            if link['rel'] == 'hub':
                return link['href']

    def _send_request(self, url, data):
        def data_generator():
            for key, value in data:
                key = 'hub.' + key
                if isinstance(value, basestring):
                    yield key, value
                else:
                    for subvalue in value:
                        yield key, value
        encoded_data = urlencode(data_generator())
        return urllib2.urlopen(url, encoded_data)

class Subscription(models.Model):

    hub = models.URLField()
    topic = models.URLField()
    verified = models.BooleanField(default=False)
    verify_token = models.CharField(max_length=60)
    lease_expires = models.DateTimeField(default=datetime.now)

    objects = SubscriptionManager()

    # class Meta:
    #     unique_together = [
    #         ('hub', 'topic')
    #         ]

    def set_expiration(self, lease_seconds):
        self.lease_expires = datetime.now() + timedelta(
            seconds=lease_seconds)
        self.save()

    def generate_token(self, mode):
        assert self.pk is not None, \
            'Subscription must be saved before generating token'
        token = mode[:20] + sha_constructor('%s%i%s' % (
                settings.SECRET_KEY, self.pk, mode)).hexdigest()
        self.verify_token = token
        self.save()
        return token
