import httplib
import urllib
import base64
import socket

from celery.utils.log import get_task_logger
log = get_task_logger(__name__)

from django.utils import simplejson
from django.conf import settings

from celery import Celery

from mixpanel.conf import settings as mp_settings

celery = Celery('mixpanel')
celery.config_from_object(settings)

@celery.task(name="mixpanel.tasks.PeopleTracker", max_retries=mp_settings.MIXPANEL_MAX_RETRIES)
def people_tracker(distinct_id, properties=None, token=None, test=None, throw_retry_error=False):
    """
    Track an event occurrence to mixpanel through the API.

    ``event_name`` is the string for the event/category you'd like to log
    this event under
    ``properties`` is (optionally) a dictionary of key/value pairs
    describing the event.
    ``token`` is (optionally) your Mixpanel api token. Not required if
    you've already configured your MIXPANEL_API_TOKEN setting.
    ``test`` is an optional override to your
    `:data:mixpanel.conf.settings.MIXPANEL_TEST_ONLY` setting for determining
    if the event requests should actually be stored on the Mixpanel servers.
    """
    log.info("Recording people datapoint: <%s>" % distinct_id)

    is_test = _is_test(test)

    url_params = _build_people_params(distinct_id, properties, is_test)
    conn = _get_connection()

    try:
        result = _send_request(conn, url_params, mp_settings.MIXPANEL_PEOPLE_TRACKING_ENDPOINT)
    except FailedEventRequest, exception:
        conn.close()
        log.info("Event failed. Retrying: user <%s>" % distinct_id)
        raise event_tracker.retry(exc=exception,
            countdown=mp_settings.MIXPANEL_RETRY_DELAY,
            throw=throw_retry_error)
    conn.close()
    return result

@celery.task(name="mixpanel.tasks.EventTracker", max_retries=mp_settings.MIXPANEL_MAX_RETRIES)
def event_tracker(event_name, properties=None, token=None, test=None, throw_retry_error=False):
    """
    Track an event occurrence to mixpanel through the API.

    ``event_name`` is the string for the event/category you'd like to log
    this event under
    ``properties`` is (optionally) a dictionary of key/value pairs
    describing the event.
    ``token`` is (optionally) your Mixpanel api token. Not required if
    you've already configured your MIXPANEL_API_TOKEN setting.
    ``test`` is an optional override to your
    `:data:mixpanel.conf.settings.MIXPANEL_TEST_ONLY` setting for determining
    if the event requests should actually be stored on the Mixpanel servers.
    """
    log.info("Recording event: <%s>" % event_name)

    is_test = _is_test(test)
    generated_properties = _handle_properties(properties, token)

    url_params = _build_params(event_name, generated_properties, is_test)
    conn = _get_connection()


    try:
        result = _send_request(conn, url_params)
    except FailedEventRequest, exception:
        conn.close()
        log.info("Event failed. Retrying: <%s>" % event_name)
        raise event_tracker.retry(exc=exception,
                   countdown=mp_settings.MIXPANEL_RETRY_DELAY,
                   throw=throw_retry_error)
    conn.close()
    return result

@celery.task(name="mixpanel.tasks.FunnelEventTracker", max_retries=mp_settings.MIXPANEL_MAX_RETRIES)
def funnel_event_tracker(funnel, step, goal, properties, token=None, test=None,
        throw_retry_error=False):
    """
    Track an event occurrence to mixpanel through the API.

    ``funnel`` is the string for the funnel you'd like to log
    this event under
    ``step`` the step in the funnel you're registering
    ``goal`` the end goal of this funnel
    ``properties`` is a dictionary of key/value pairs
    describing the funnel event. A ``distinct_id`` is required.
    ``token`` is (optionally) your Mixpanel api token. Not required if
    you've already configured your MIXPANEL_API_TOKEN setting.
    ``test`` is an optional override to your
    `:data:mixpanel.conf.settings.MIXPANEL_TEST_ONLY` setting for determining
    if the event requests should actually be stored on the Mixpanel servers.
    """
    log.info("Recording funnel: <%s>-<%s>" % (funnel, step))
    properties = _handle_properties(properties, token)

    is_test = _is_test(test)
    properties = _add_funnel_properties(properties, funnel, step, goal)

    url_params = _build_params(mp_settings.MIXPANEL_FUNNEL_EVENT_ID,
                                    properties, is_test)
    conn = _get_connection()

    try:
        result = _send_request(conn, url_params)
    except FailedEventRequest, exception:
        conn.close()
        log.info("Funnel failed. Retrying: <%s>-<%s>" % (funnel, step))
        raise funnel_event_tracker.retry(exc=exception,
                   countdown=mp_settings.MIXPANEL_RETRY_DELAY,
                   throw=throw_retry_error)
    conn.close()
    return result

class FailedEventRequest(Exception):
    """The attempted recording event failed because of a non-200 HTTP return code"""
    pass

class InvalidFunnelProperties(Exception):
    """Required properties were missing from the funnel-tracking call"""
    pass

def _is_test(test):
    """
    Determine whether this event should be logged as a test request, meaning
    it won't actually be stored on the Mixpanel servers. A return result of
    1 means this will be a test, 0 means it won't as per the API spec.

    Uses ``:mod:mixpanel.conf.settings.MIXPANEL_TEST_ONLY`` as the default
    if no explicit test option is given.
    """
    if test == None:
        test = mp_settings.MIXPANEL_TEST_ONLY

    if test:
        return 1
    return 0

def _handle_properties(properties, token):
    """
    Build a properties dictionary, accounting for the token.
    """
    if properties == None:
        properties = {}

    if not properties.get('token', None):
        if token is None:
            token = mp_settings.MIXPANEL_API_TOKEN
        properties['token'] = token

    return properties

def _get_connection():
    server = mp_settings.MIXPANEL_API_SERVER

    # Wish we could use python 2.6's httplib timeout support
    socket.setdefaulttimeout(mp_settings.MIXPANEL_API_TIMEOUT)
    return httplib.HTTPConnection(server)

def _build_people_params(distinct_id, properties, is_test):
    """
    Build HTTP params to record the given event and properties.
    """
    params = {'$distinct_id': distinct_id,'$token': mp_settings.MIXPANEL_API_TOKEN}
    if 'set' in properties:
        #adding $ to any reserved mixpanel vars
        for special_prop in mp_settings.MIXPANEL_RESERVED_PEOPLE_PROPERTIES:
            if special_prop in properties['set']:
                properties['set']['${}'.format(special_prop)] = properties['set'][special_prop]
                del properties['set'][special_prop]
        params['$set'] = properties['set']
    if 'increment' in properties:
        params['$add'] = properties['increment']
    data = base64.b64encode(simplejson.dumps(params))

    data_var = mp_settings.MIXPANEL_DATA_VARIABLE
    url_params = urllib.urlencode({data_var: data, 'test': is_test})

    return url_params

def _build_params(event, properties, is_test):
    """
    Build HTTP params to record the given event and properties.
    """
    params = {'event': event, 'properties': properties}
    data = base64.b64encode(simplejson.dumps(params))

    data_var = mp_settings.MIXPANEL_DATA_VARIABLE
    url_params = urllib.urlencode({data_var: data, 'test': is_test})

    return url_params

def _send_request(connection, params, endpoint=mp_settings.MIXPANEL_TRACKING_ENDPOINT):
    """
    Send a an event with its properties to the api server.

    Returns ``true`` if the event was logged by Mixpanel.
    """
    try:
        connection.request('GET', '%s?%s' % (endpoint, params))

        response = connection.getresponse()
    except socket.error, message:
        raise FailedEventRequest("The tracking request failed with a socket error. Message: [%s]" % message)

    if response.status != 200 or response.reason != 'OK':
        raise FailedEventRequest("The tracking request failed. Non-200 response code was: %s %s" % (response.status, response.reason))

    # Successful requests will generate a log
    response_data = response.read()
    if response_data != '1':
        return False

    return True

def _add_funnel_properties(properties, funnel, step, goal):
    if not 'distinct_id' in properties:
        error_msg = "A ``distinct_id`` must be given to record a funnel event"
        raise InvalidFunnelProperties(error_msg)
    properties['funnel'] = funnel
    properties['step'] = step
    properties['goal'] = goal

    return properties

