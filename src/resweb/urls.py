from django.conf.urls.defaults import *

# Uncomment the next two lines to enable the admin:
# from django.contrib import admin
# admin.autodiscover()

urlpatterns = patterns('',
    # Example:
    # (r'^resweb/', include('resweb.foo.urls')),
    url(r'^$', 'resweb.core.views.index', name='overview'),
    url(r'^working/$', 'resweb.core.views.working', name='working'),
    url(r'^queues/$', 'resweb.core.views.queues', name='queues'),
    url(r'^queues/(?P<queue_id>\w+)/$', 'resweb.core.views.queue_detail', name='queue_detail'),
    url(r'^failed/$', 'resweb.core.views.failed', name='failed'),
    url(r'^workers/$', 'resweb.core.views.workers', name='workers'),
    url(r'^stats/$', 'resweb.core.views.stats', name='stats'),
    # Uncomment the admin/doc line below and add 'django.contrib.admindocs' 
    # to INSTALLED_APPS to enable admin documentation:
    # (r'^admin/doc/', include('django.contrib.admindocs.urls')),

    # Uncomment the next line to enable the admin:
    # (r'^admin/', include(admin.site.urls)),
)
