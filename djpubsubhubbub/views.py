from datetime import datetime
import feedparser

from django.http import HttpResponse, Http404
from django.shortcuts import get_object_or_404

from djpubsubhubbub.models import Subscription
from djpubsubhubbub.signals import verified, updated

def callback(request, pk):
    if request.method == 'GET':
        mode = request.GET['hub.mode']
        topic = request.GET['hub.topic']
        challenge = request.GET['hub.challenge']
        lease_seconds = request.GET.get('hub.lease_seconds')
        verify_token = request.GET.get('hub.verify_token', '')

        if mode == 'subscribe':
            if not verify_token.startswith('subscribe'):
                raise Http404
            subscription = get_object_or_404(Subscription,
                                             pk=pk,
                                             topic=topic,
                                             verify_token=verify_token)
            subscription.verified = True
            subscription.set_expiration(int(lease_seconds))
            verified.send(sender=subscription)

        return HttpResponse(challenge, content_type='text/plain')
    elif request.method == 'POST':
        subscription = get_object_or_404(Subscription, pk=pk)
        parsed = feedparser.parse(request.raw_post_data)
        if parsed.feed.links: # single notification
            hub_url = subscription.hub
            self_url = subscription.topic
            for link in parsed.feed.links:
                if link['rel'] == 'hub':
                    hub_url = link['href']
                elif link['rel'] == 'self':
                    self_url = link['href']

            needs_update = False
            if hub_url and subscription.hub != hub_url:
                # hub URL has changed; let's update our subscription
                needs_update = True
            elif self_url != subscription.topic:
                # topic URL has changed
                needs_update = True

            if needs_update:
                expiration_time = datetime.now() - subscription.lease_expires
                seconds = expiration_time.days*86400 + expiration_time.seconds
                Subscription.objects.subscribe(
                    self_url, hub_url,
                    callback=request.get_full_path(),
                    lease_seconds=seconds)

            updated.send(sender=subscription, update=parsed)
            return HttpResponse('')
    return Http404
