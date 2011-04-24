import warnings

from utils import rc
from django.core.exceptions import ObjectDoesNotExist, MultipleObjectsReturned
from django.conf import settings
from django.db.models.fields.related import ForeignKey

typemapper = { }
handler_tracker = [ ]

class HandlerMetaClass(type):
    """
    Metaclass that keeps a registry of class -> handler
    mappings.
    """
    def __new__(cls, name, bases, attrs):
        new_cls = type.__new__(cls, name, bases, attrs)

        def already_registered(model, anon):
            for k, (m, a) in typemapper.iteritems():
                if model == m and anon == a:
                    return k

        if hasattr(new_cls, 'model'):
            if already_registered(new_cls.model, new_cls.is_anonymous):
                if not getattr(settings, 'PISTON_IGNORE_DUPE_MODELS', False):
                    warnings.warn("Handler already registered for model %s, "
                        "you may experience inconsistent results." % new_cls.model.__name__)

            typemapper[new_cls] = (new_cls.model, new_cls.is_anonymous)
        else:
            typemapper[new_cls] = (None, new_cls.is_anonymous)

        if name not in ('BaseHandler', 'AnonymousBaseHandler'):
            handler_tracker.append(new_cls)

        return new_cls

class BaseHandler(object):
    """
    Basehandler that gives you CRUD for free.
    You are supposed to subclass this for specific
    functionality.

    All CRUD methods (`read`/`update`/`create`/`delete`)
    receive a request as the first argument from the
    resource. Use this for checking `request.user`, etc.
    """
    __metaclass__ = HandlerMetaClass

    allowed_methods = ('GET', 'POST', 'PUT', 'DELETE')
    anonymous = is_anonymous = False
    exclude = ( 'id', )
    fields =  ( )
    _instance = None
    
    def _resolve_fk(self, klass, key):
        if key in self.dct:
            try:
                self.dct[key] = klass.objects.get(pk=self.dct[key])
            except klass.DoesNotExist:
                resp = rc.BAD_REQUEST
                resp.write('%s with primary key "%d" not found.' % (klass.__name__, self.dct[key]))
    
    def _handle_m2m(self, model, field_name):
        m2m_attr = getattr(self._instance, field_name)
        remove_key = field_name + '__remove'
        add_key = field_name + '__add'

        if remove_key in self.dct:
            pk_set = [int(pk) for pk in self.dct[remove_key].split(',')]
            for pk in pk_set:
                try:
                    m2m_attr.remove(model.objects.get(pk=pk))
                except model.DoesNotExist:
                    continue
            del self.dct[remove_key]
        elif add_key in self.dct:
            pk_set = [int(pk) for pk in self.dct[add_key].split(',')]
            for pk in pk_set:
                try:
                    m2m_attr.add(model.objects.get(pk=pk))
                except model.DoesNotExist:
                    continue
            del self.dct[add_key]
    
    def _resolve_extras(self, request):
        if request.data:
            self.dct = request.data.copy()
            self.error = None

            for field in self.model._meta.fields:
                if isinstance(field, ForeignKey):
                    self._resolve_fk(field.related.parent_model, field.name)

            if self._instance is not None:
                for field, model in self.model._meta.get_m2m_with_model():
                    self._handle_m2m(field.related.parent_model, field.name)

            request.data = self.dct
            return self.error or None

    def flatten_dict(self, dct):
        result = dict([ (str(k), dct.get(k)) for k in dct.keys() ])
        if 'csrfmiddlewaretoken' in result:
            del result['csrfmiddlewaretoken']
        return result

    def has_model(self):
        return hasattr(self, 'model') or hasattr(self, 'queryset')

    def queryset(self, request):
        return self.model.objects.all()

    def value_from_tuple(tu, name):
        for int_, n in tu:
            if n == name:
                return int_
        return None

    def exists(self, **kwargs):
        if not self.has_model():
            raise NotImplementedError

        try:
            self.model.objects.get(**kwargs)
            return True
        except self.model.DoesNotExist:
            return False

    def read(self, request, *args, **kwargs):
        if not self.has_model():
            return rc.NOT_IMPLEMENTED

        pkfield = self.model._meta.pk.name

        if pkfield in kwargs:
            try:
                return self.queryset(request).get(pk=kwargs.get(pkfield))
            except ObjectDoesNotExist:
                return rc.NOT_FOUND
            except MultipleObjectsReturned: # should never happen, since we're using a PK
                return rc.BAD_REQUEST
        else:
            return self.queryset(request).filter(*args, **kwargs)

    def create(self, request, *args, **kwargs):
        error = self._resolve_extras(request)
        if error:
            return error
            
        if not self.has_model():
            return rc.NOT_IMPLEMENTED

        attrs = self.flatten_dict(request.data)

        try:
            inst = self.queryset(request).get(**attrs)
            return rc.DUPLICATE_ENTRY
        except self.model.DoesNotExist:
            inst = self.model(**attrs)
            inst.save()
            return inst
        except self.model.MultipleObjectsReturned:
            return rc.DUPLICATE_ENTRY

    def update(self, request, *args, **kwargs):
        if not self.has_model():
            return rc.NOT_IMPLEMENTED

        pkfield = self.model._meta.pk.name

        if pkfield not in kwargs:
            # No pk was specified
            return rc.BAD_REQUEST

        try:
            self._instance = self.queryset(request).get(pk=kwargs.get(pkfield))
        except ObjectDoesNotExist:
            return rc.NOT_FOUND
        except MultipleObjectsReturned: # should never happen, since we're using a PK
            return rc.BAD_REQUEST
        
        error = self._resolve_extras(request)
        if error:
            return error

        attrs = self.flatten_dict(request.data)
        for k,v in attrs.iteritems():
            setattr( inst, k, v )

        self._instance.save()
        return rc.ALL_OK

    def delete(self, request, *args, **kwargs):
        if not self.has_model():
            raise NotImplementedError

        try:
            inst = self.queryset(request).get(*args, **kwargs)

            inst.delete()

            return rc.DELETED
        except self.model.MultipleObjectsReturned:
            return rc.DUPLICATE_ENTRY
        except self.model.DoesNotExist:
            return rc.NOT_HERE

class AnonymousBaseHandler(BaseHandler):
    """
    Anonymous handler.
    """
    is_anonymous = True
    allowed_methods = ('GET',)
