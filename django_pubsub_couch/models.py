# Copyright 2009 - Participatory Culture Foundation
#
# This file is part of django-pubsub-couch.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions
# are met:
#
# 1. Redistributions of source code must retain the above copyright
#    notice, this list of conditions and the following disclaimer.
# 2. Redistributions in binary form must reproduce the above copyright
#    notice, this list of conditions and the following disclaimer in the
#    documentation and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR
# IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
# OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
# IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT,
# INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
# NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
# DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
# THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
# THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

from datetime import datetime, timedelta
import feedparser
from urllib import urlencode
import urllib2

from couchdbkit.ext.django.schema import *
from couchdbkit import ResourceConflict

from django.conf import settings
from django.contrib.sites.models import RequestSite
from django.core.urlresolvers import reverse, Resolver404
from hashlib import sha1

from django_pubsub_couch import signals

DEFAULT_LEASE_SECONDS = 2592000 # 30 days in seconds

class SubscriptionManager(object):

    def get_or_create(self, hub, topic):

        myid = '%s-%s' % (sha1(hub).hexdigest(), sha1(topic).hexdigest())

        try:
            subscription = Subscription(_id=myid, hub=hub, topic=topic)
            subscription.save()
            created = True

        except ResourceConflict:
            subscription = Subscription.get(myid)
            created = False

        return subscription, created


    def get(self, **kwargs):
        if 'pk' in kwargs:
            pk = kwargs['pk']
        else:
            topic, hub = kwargs['topic'], kwargs['hub']
            pk = '%s-%s' % (sha1(hub).hexdigest(), sha1(topic).hexdigest())
        subscription = Subscription.get(pk)
        return subscription


    def delete(self, **kwargs):
        if 'pk' in kwargs:
            pk = kwargs['pk']
        else:
            topic, hub = kwargs['topic'], kwargs['hub']
            pk = '%s-%s' % (sha1(hub).hexdigest(), sha1(topic).hexdigest())
        subscription = Subscription.get(pk)
        subscription.delete()


    def create(self, **kwargs):
        topic, hub = kwargs['topic'], kwargs['hub']
        myid = '%s-%s' % (sha1(hub).hexdigest(), sha1(topic).hexdigest())
        subscription = Subscription(_id=myid, **kwargs)
        subscription.save()
        return subscription


    def subscribe(self, topic, hub=None, callback=None,
                  lease_seconds=None, request=None):
        if hub is None:
            hub = self._get_hub(topic)

        if hub is None:
            raise TypeError(
                'hub cannot be None if the feed does not provide it')

        if lease_seconds is None:
            lease_seconds = getattr(settings, 'PUBSUBHUBBUB_LEASE_SECONDS',
                                   DEFAULT_LEASE_SECONDS)

        subscription, created = self.get_or_create(hub, topic)

        signals.pre_subscribe.send(sender=subscription, created=created)
        subscription.set_expiration(lease_seconds)

        if callback is None:
            try:
                callback_path = reverse('pubsubhubbub_callback',
                                        args=(subscription._id,))
            except Resolver404:
                raise TypeError(
                    'callback cannot be None if there is not a reverable URL')
            else:
                if not request:
                    raise TypeError(
                        'either callback or request must be provided')
                site = RequestSite(request)
                callback = 'http://' + site.domain + callback_path

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
        if subscription.verified:
            signals.verified.send(sender=subscription)
        return subscription


    def _get_hub(self, topic):
        parsed = feedparser.parse(topic)
        for link in parsed.feed.links:
            if link['rel'] == 'hub':
                return link['href']

    def _send_request(self, url, data):
        def data_generator():
            for key, value in data.items():
                key = 'hub.' + key
                if isinstance(value, (basestring, int)):
                    yield key, str(value)
                else:
                    for subvalue in value:
                        yield key, value
        encoded_data = urlencode(list(data_generator()))
        return urllib2.urlopen(url, encoded_data)

class Subscription(Document):

    hub = StringProperty()
    topic = StringProperty()
    verified = BooleanProperty()
    verify_token = StringProperty()
    lease_expires = DateTimeProperty(default=datetime.now)

    objects = SubscriptionManager()

    def set_expiration(self, lease_seconds):
        self.lease_expires = datetime.now() + timedelta(
            seconds=lease_seconds)
        self.save()

    def generate_token(self, mode):
        assert self._id is not None, \
            'Subscription must be saved before generating token'
        token = mode[:20] + sha1('%s%s%s' % (
                settings.SECRET_KEY, self._id, mode)).hexdigest()
        self.verify_token = token
        self.save()
        return token

    def __unicode__(self):
        if self.verified:
            verified = u'verified'
        else:
            verified = u'unverified'
        return u'to %s on %s: %s' % (
            self.topic, self.hub, verified)

    def __str__(self):
        return str(unicode(self))

    @property
    def pk(self):
        return self._id


    def __eq__(self, other):
        return (self._id is not None) and (self._id == other._id)
