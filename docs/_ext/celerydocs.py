import pytest
from unittest.mock import Mock, MagicMock, patch
from docutils import nodes
from sphinx.errors import NoUri

from docs._ext.celerydocs import (
    typeify, shorten, get_abbr, resolve, pkg_of, basename,
    modify_textnode, maybe_resolve_abbreviations, setup,
    APPATTRS, ABBRS, ABBR_EMPTY, DEFAULT_EMPTY
)


class TestTypeify:
    """Test cases for typeify function."""
    
    def test_typeify_with_meth_type(self):
        """Test typeify adds parentheses for method type."""
        result = typeify("test_method", "meth")
        assert result == "test_method()"
    
    def test_typeify_with_func_type(self):
        """Test typeify adds parentheses for function type."""
        result = typeify("test_function", "func")
        assert result == "test_function()"
    
    def test_typeify_with_other_type(self):
        """Test typeify returns unchanged string for other types."""
        result = typeify("test_class", "class")
        assert result == "test_class"
    
    def test_typeify_with_empty_string(self):
        """Test typeify handles empty string."""
        result = typeify("", "meth")
        assert result == "()"


class TestShorten:
    """Test cases for shorten function."""
    
    def test_shorten_with_at_dash_prefix(self):
        """Test shorten removes @- prefix."""
        result = shorten("@-test.method", "target", APPATTRS)
        assert result == "test.method"
    
    def test_shorten_with_at_prefix_appattrs(self):
        """Test shorten with @ prefix and APPATTRS dict."""
        result = shorten("@test", "target", APPATTRS)
        assert result == "app.test"
    
    def test_shorten_with_at_prefix_other_dict(self):
        """Test shorten with @ prefix and non-APPATTRS dict."""
        result = shorten("@test", "target", ABBRS)
        assert result == "test"
    
    def test_shorten_without_prefix(self):
        """Test shorten returns unchanged string without prefix."""
        result = shorten("test.method", "target", APPATTRS)
        assert result == "test.method"


class TestGetAbbr:
    """Test cases for get_abbr function."""
    
    def test_get_abbr_with_valid_appattrs_pre(self):
        """Test get_abbr with valid prefix in APPATTRS."""
        result = get_abbr("amqp", "method", "meth")
        expected = (APPATTRS["amqp"], "method", APPATTRS)
        assert result == expected
    
    def test_get_abbr_with_valid_abbrs_pre(self):
        """Test get_abbr with valid prefix in ABBRS."""
        result = get_abbr("Celery", "method", "class")
        expected = (ABBRS["Celery"], "method", ABBRS)
        assert result == expected
    
    def test_get_abbr_with_invalid_pre(self):
        """Test get_abbr raises KeyError for invalid prefix."""
        with pytest.raises(KeyError, match="Unknown abbreviation"):
            get_abbr("invalid", "method", "meth")
    
    def test_get_abbr_without_pre_valid_rest(self):
        """Test get_abbr without prefix but valid rest in APPATTRS."""
        result = get_abbr("", "amqp", "meth")
        expected = (APPATTRS["amqp"], "", APPATTRS)
        assert result == expected
    
    def test_get_abbr_without_pre_invalid_rest(self):
        """Test get_abbr without prefix and invalid rest."""
        result = get_abbr("", "invalid", "exc")
        expected = (ABBR_EMPTY["exc"], "invalid", ABBR_EMPTY)
        assert result == expected
    
    def test_get_abbr_without_pre_default_type(self):
        """Test get_abbr without prefix using default type."""
        result = get_abbr("", "invalid", "unknown")
        expected = (DEFAULT_EMPTY, "invalid", ABBR_EMPTY)
        assert result == expected


class TestResolve:
    """Test cases for resolve function."""
    
    def test_resolve_typing_module(self):
        """Test resolve with typing module attribute."""
        result = resolve("List", "class")
        assert result == ("typing.List", None)
    
    def test_resolve_non_typing_no_dot(self):
        """Test resolve with non-typing single word."""
        result = resolve("InvalidType", "class")
        assert result == ("InvalidType", None)
    
    def test_resolve_with_dot_no_at(self):
        """Test resolve with dotted name without @ prefix."""
        result = resolve("module.Class", "class")
        assert result == ("module.Class", None)
    
    def test_resolve_with_at_prefix_valid_abbr(self):
        """Test resolve with @ prefix and valid abbreviation."""
        result = resolve("@amqp.method", "meth")
        expected_target = f"{APPATTRS['amqp']}.method"
        assert result == (expected_target, APPATTRS)
    
    def test_resolve_with_at_dash_prefix(self):
        """Test resolve with @- prefix."""
        result = resolve("@-amqp.method", "meth")
        expected_target = f"{APPATTRS['amqp']}.method"
        assert result == (expected_target, APPATTRS)
    
    def test_resolve_with_at_no_dot(self):
        """Test resolve with @ prefix but no dot."""
        result = resolve("@amqp", "meth")
        expected_target = APPATTRS['amqp']
        assert result == (expected_target, APPATTRS)
    
    def test_resolve_with_invalid_abbr(self):
        """Test resolve with invalid abbreviation raises KeyError."""
        with pytest.raises(KeyError):
            resolve("@invalid.method", "meth")


class TestPkgOf:
    """Test cases for pkg_of function."""
    
    def test_pkg_of_simple_module(self):
        """Test pkg_of with simple module name."""
        result = pkg_of("celery")
        assert result == "celery"
    
    def test_pkg_of_dotted_module(self):
        """Test pkg_of with dotted module name."""
        result = pkg_of("celery.app.base")
        assert result == "celery"
    
    def test_pkg_of_empty_string(self):
        """Test pkg_of with empty string."""
        result = pkg_of("")
        assert result == ""


class TestBasename:
    """Test cases for basename function."""
    
    def test_basename_simple_name(self):
        """Test basename with simple name."""
        result = basename("Class")
        assert result == "Class"
    
    def test_basename_dotted_name(self):
        """Test basename with dotted name."""
        result = basename("module.submodule.Class")
        assert result == "Class"
    
    def test_basename_with_at_prefix(self):
        """Test basename removes @ prefix."""
        result = basename("@module.Class")
        assert result == "Class"
    
    def test_basename_empty_string(self):
        """Test basename with empty string."""
        result = basename("")
        assert result == ""


class TestModifyTextnode:
    """Test cases for modify_textnode function."""
    
    def test_modify_textnode_with_tilde(self):
        """Test modify_textnode with tilde in rawsource."""
        mock_text = Mock()
        mock_text.rawsource = "~method"
        mock_node = Mock()
        mock_node.children = [mock_text]
        
        result = modify_textnode("target.method", "new.target.method", 
                               mock_node, APPATTRS, "meth")
        
        assert isinstance(result, nodes.Text)
        assert result.astext() == "method()"
    
    def test_modify_textnode_without_tilde(self):
        """Test modify_textnode without tilde in rawsource."""
        mock_text = Mock()
        mock_text.rawsource = "method"
        mock_node = Mock()
        mock_node.children = [mock_text]
        
        result = modify_textnode("@target.method", "new.target.method", 
                               mock_node, APPATTRS, "meth")
        
        assert isinstance(result, nodes.Text)
        assert result.astext() == "app.target.method()"
    
    def test_modify_textnode_non_function_type(self):
        """Test modify_textnode with non-function type."""
        mock_text = Mock()
        mock_text.rawsource = "Class"
        mock_node = Mock()
        mock_node.children = [mock_text]
        
        result = modify_textnode("target.Class", "new.target.Class", 
                               mock_node, ABBRS, "class")
        
        assert isinstance(result, nodes.Text)
        assert result.astext() == "target.Class"


class TestMaybeResolveAbbreviations:
    """Test cases for maybe_resolve_abbreviations function."""
    
    def test_maybe_resolve_abbreviations_non_at_target(self):
        """Test function returns None for non-@ targets."""
        mock_node = {'reftarget': 'normal.target', 'reftype': 'meth'}
        result = maybe_resolve_abbreviations(None, None, mock_node, None)
        assert result is None
    
    def test_maybe_resolve_abbreviations_with_at_target(self):
        """Test function processes @ targets."""
        mock_app = Mock()
        mock_env = Mock()
        mock_contnode = Mock()
        mock_text = Mock()
        mock_text.rawsource = "@amqp.method"
        mock_contnode.__len__ = Mock(return_value=1)
        mock_contnode.__getitem__ = Mock(return_value=mock_text)
        mock_contnode.__setitem__ = Mock()
        
        mock_node = {
            'reftarget': '@amqp.method',
            'reftype': 'meth',
            'refdomain': 'py',
            'refdoc': 'test'
        }
        
        mock_domain = Mock()
        mock_domain.resolve_xref = Mock(return_value="resolved_ref")
        mock_env.domains = {'py': mock_domain}
        
        result = maybe_resolve_abbreviations(mock_app, mock_env, mock_node, mock_contnode)
        
        # Verify the target was updated
        assert mock_node['reftarget'] == APPATTRS['amqp'] + '.method'
        assert result == "resolved_ref"
    
    def test_maybe_resolve_abbreviations_no_domain(self):
        """Test function with no domain specified."""
        mock_node = {
            'reftarget': '@amqp.method',
            'reftype': 'meth',
            'refdomain': None
        }
        mock_contnode = []
        
        result = maybe_resolve_abbreviations(None, None, mock_node, mock_contnode)
        assert result is None
    
    def test_maybe_resolve_abbreviations_domain_keyerror(self):
        """Test function raises NoUri when domain not found."""
        mock_env = Mock()
        mock_env.domains = {}
        mock_node = {
            'reftarget': '@amqp.method',
            'reftype': 'meth',
            'refdomain': 'py'
        }
        mock_contnode = []
        
        with pytest.raises(NoUri):
            maybe_resolve_abbreviations(None, mock_env, mock_node, mock_contnode)


class TestSetup:
    """Test cases for setup function."""
    
    def test_setup_connects_missing_reference(self):
        """Test setup connects missing-reference event."""
        mock_app = Mock()
        
        result = setup(mock_app)
        
        # Verify missing-reference was connected
        mock_app.connect.assert_called_with(
            'missing-reference',
            maybe_resolve_abbreviations,
        )
        
        # Verify crossref types were added
        assert mock_app.add_crossref_type.call_count == 4
        
        # Verify return value
        assert result == {'parallel_read_safe': True}
    
    def test_setup_adds_crossref_types(self):
        """Test setup adds all required crossref types."""
        mock_app = Mock()
        
        setup(mock_app)
        
        # Check that all expected crossref types were added
        expected_calls = [
            (('sig', 'sig', 'pair: %s; sig'), {}),
            (('state', 'state', 'pair: %s; state'), {}),
            (('control', 'control', 'pair: %s; control'), {}),
            (('event', 'event', 'pair: %s; event'), {}),
        ]
        
        actual_calls = [(call.args[:3], call.kwargs) for call in mock_app.add_crossref_type.call_args_list]
        
        for expected_call in expected_calls:
            assert expected_call in actual_calls
import typing

from docutils import nodes
from sphinx.errors import NoUri

APPATTRS = {
    'amqp': 'celery.app.amqp.AMQP',
    'backend': 'celery.backends.base.BaseBackend',
    'conf': 'celery.app.utils.Settings',
    'control': 'celery.app.control.Control',
    'events': 'celery.events.Events',
    'loader': 'celery.app.loaders.base.BaseLoader',
    'log': 'celery.app.log.Logging',
    'pool': 'kombu.connection.ConnectionPool',
    'tasks': 'celery.app.registry.Registry',

    'AsyncResult': 'celery.result.AsyncResult',
    'ResultSet': 'celery.result.ResultSet',
    'GroupResult': 'celery.result.GroupResult',
    'Worker': 'celery.apps.worker.Worker',
    'WorkController': 'celery.worker.WorkController',
    'Beat': 'celery.apps.beat.Beat',
    'Task': 'celery.app.task.Task',
    'signature': 'celery.canvas.Signature',
}

APPDIRECT = {
    'on_configure', 'on_after_configure', 'on_after_finalize',
    'set_current', 'set_default', 'close', 'on_init', 'start',
    'worker_main', 'task', 'gen_task_name', 'finalize',
    'add_defaults', 'config_from_object', 'config_from_envvar',
    'config_from_cmdline', 'setup_security', 'autodiscover_tasks',
    'send_task', 'connection', 'connection_or_acquire',
    'producer_or_acquire', 'prepare_config', 'now',
    'select_queues', 'either', 'bugreport', 'create_task_cls',
    'subclass_with_self', 'annotations', 'current_task', 'oid',
    'timezone', '__reduce_keys__', 'fixups', 'finalized', 'configured',
    'add_periodic_task',
    'autofinalize', 'steps', 'user_options', 'main', 'clock',
}

APPATTRS.update({x: f'celery.Celery.{x}' for x in APPDIRECT})

ABBRS = {
    'Celery': 'celery.Celery',
}

ABBR_EMPTY = {
    'exc': 'celery.exceptions',
}
DEFAULT_EMPTY = 'celery.Celery'


def typeify(S, type):
    if type in ('meth', 'func'):
        return S + '()'
    return S


def shorten(S, newtarget, src_dict):
    if S.startswith('@-'):
        return S[2:]
    elif S.startswith('@'):
        if src_dict is APPATTRS:
            return '.'.join(['app', S[1:]])
        return S[1:]
    return S


def get_abbr(pre, rest, type, orig=None):
    if pre:
        for d in APPATTRS, ABBRS:
            try:
                return d[pre], rest, d
            except KeyError:
                pass
        raise KeyError('Unknown abbreviation: {} ({})'.format(
            '.'.join([pre, rest]) if orig is None else orig, type,
        ))
    else:
        for d in APPATTRS, ABBRS:
            try:
                return d[rest], '', d
            except KeyError:
                pass
    return ABBR_EMPTY.get(type, DEFAULT_EMPTY), rest, ABBR_EMPTY


def resolve(S, type):
    if '.' not in S:
        try:
            getattr(typing, S)
        except AttributeError:
            pass
        else:
            return f'typing.{S}', None
    orig = S
    if S.startswith('@'):
        S = S.lstrip('@-')
        try:
            pre, rest = S.split('.', 1)
        except ValueError:
            pre, rest = '', S

        target, rest, src = get_abbr(pre, rest, type, orig)
        return '.'.join([target, rest]) if rest else target, src
    return S, None


def pkg_of(module_fqdn):
    return module_fqdn.split('.', 1)[0]


def basename(module_fqdn):
    return module_fqdn.lstrip('@').rsplit('.', -1)[-1]


def modify_textnode(T, newtarget, node, src_dict, type):
    src = node.children[0].rawsource
    return nodes.Text(
        (typeify(basename(T), type) if '~' in src
         else typeify(shorten(T, newtarget, src_dict), type)),
        src,
    )


def maybe_resolve_abbreviations(app, env, node, contnode):
    domainname = node.get('refdomain')
    target = node['reftarget']
    type = node['reftype']
    if target.startswith('@'):
        newtarget, src_dict = resolve(target, type)
        node['reftarget'] = newtarget
        # shorten text if '~' is not enabled.
        if len(contnode) and isinstance(contnode[0], nodes.Text):
            contnode[0] = modify_textnode(target, newtarget, node,
                                          src_dict, type)
        if domainname:
            try:
                domain = env.domains[node.get('refdomain')]
            except KeyError:
                raise NoUri
            try:
                return domain.resolve_xref(env, node['refdoc'], app.builder,
                                           type, newtarget,
                                           node, contnode)
            except KeyError:
                raise NoUri


def setup(app):
    app.connect(
        'missing-reference',
        maybe_resolve_abbreviations,
    )

    app.add_crossref_type(
        directivename='sig',
        rolename='sig',
        indextemplate='pair: %s; sig',
    )
    app.add_crossref_type(
        directivename='state',
        rolename='state',
        indextemplate='pair: %s; state',
    )
    app.add_crossref_type(
        directivename='control',
        rolename='control',
        indextemplate='pair: %s; control',
    )
    app.add_crossref_type(
        directivename='event',
        rolename='event',
        indextemplate='pair: %s; event',
    )

    return {
        'parallel_read_safe': True
    }
