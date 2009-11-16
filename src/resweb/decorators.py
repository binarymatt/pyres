from django.shortcuts import render_to_response
from django.http import HttpResponseRedirect, HttpResponse
from django.template import RequestContext

def render_to(view, post_to=None):
    def wrapper(fn):
        def inner(request, *args, **kwargs):
            # POSTs may be handled by their own function, which then
            # redirects back to a GET (or to another path if required)
            if post_to and request.method == 'POST':
                result = post_to(request, *args, **kwargs)
                return HttpResponseRedirect(result or str(request.path))
            
            result = fn(request, *args, **kwargs)
            
            # If result is some sort of HttpResponse, simply return it
            if isinstance(result, HttpResponse):
                return result
            # ...but if it's a string, redirect to it
            # (a blank string means 'curent path')
            elif isinstance(result, str):
                return HttpResponseRedirect(result or request.path)
            # ...otherwise, it should be the dict for the view
            else:
                return render_to_response(view, result,
                        context_instance=RequestContext(request))
        return inner
    return wrapper
