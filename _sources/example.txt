Example
=========

Let's take a real wold example of a blog where comments need to be checked for
spam. When the comment is saved in the database, we create a job in the
queue with that comment data. Let's take a django model in this case.

.. code-block:: python
   :linenos:

    class Comment(models.Model):
        name = Model.CharField()
        email = Model.EmailField()
        body = Model.TextField()
        spam = Model.BooleanField()
        queue = "Spam"
    
        @staticmethod
        def perform(comment_id):
            comment = Comment.objects.get(pk=comment_id)
            params = {"comment_author_email": comment.user.email, 
                      "comment_content": comment.body,
                      "comment_author_name": comment.user.name,
                      "request_ip": comment.author_ip}
            x = urllib.urlopen("http://apikey.rest.akismet.com/1.1/comment-check", params)
            if x == "true":
                comment.spam = True
            else:
                comment.spam = False
            comment.save()

You can convert your existing class to be compatible with pyres. All you need 
to do is add a :attr:`queue` attribute and define a :meth:`perform` method on the class. 

To insert a job into the queue you need to do something like this::

    >>> from pyres import ResQ
    >>> r = Resq()
    >>> r.enqueue(Spam, 23)   # Passing the comment id 23

This puts a job into the queue **Spam**. Now we need to fire off our workers. 
In the **scripts** folder there is an executable::

    $ ./pyres_worker Spam


Just pass a comma separated list of queues the worker should poll.


