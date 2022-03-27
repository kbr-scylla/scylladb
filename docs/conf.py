# -*- coding: utf-8 -*-
import os
import sys
from datetime import date
import recommonmark
from recommonmark.transform import AutoStructify
from pygments.lexers.javascript import JavascriptLexer
from sphinx_scylladb_theme.utils import multiversion_regex_builder

sys.path.insert(0, os.path.abspath('..'))

# -- General configuration ------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    'sphinx.ext.todo',
    'sphinx.ext.mathjax',
    'sphinx.ext.githubpages',
    'sphinx.ext.extlinks',
    'sphinx_scylladb_theme',
    'sphinx_multiversion',
    'recommonmark',
    'sphinx_markdown_tables',
]

# The suffix(es) of source filenames.
# You can specify multiple suffix as a list of string:
#
source_suffix = ['.rst', '.md']
autosectionlabel_prefix_document = True

# The master toctree document.
master_doc = 'contents'

# General information about the project.
project = 'Scylla Documentation'
copyright = str(date.today().year) + ', ScyllaDB. All rights reserved.'
author = u'Scylla Project Contributors'

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This patterns also effect to html_static_path and html_extra_path
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store', 'README.md', '_utils']

# The name of the Pygments (syntax highlighting) style to use.
pygments_style = 'sphinx'

# -- Options for not found extension -------------------------------------------

# Template used to render the 404.html generated by this extension.
notfound_template =  '404.html'

# Prefix added to all the URLs generated in the 404 page.
notfound_urls_prefix = ''

# -- Options for redirect extension ---------------------------------------

# Read a YAML dictionary of redirections and generate an HTML file for each
redirects_file = "_utils/redirections.yaml"

# -- Options for multiversion extension ----------------------------------
# Whitelist pattern for tags (set to None to ignore all tags)
TAGS = []
smv_tag_whitelist = multiversion_regex_builder(TAGS)
# Whitelist pattern for branches (set to None to ignore all branches)
BRANCHES = ['branch-4.5', 'branch-4.6', 'master']
smv_branch_whitelist = multiversion_regex_builder(BRANCHES)
# Defines which version is considered to be the latest stable version.
smv_latest_version = 'branch-4.6'
smv_rename_latest_version = 'stable'
# Must be listed in smv_tag_whitelist or smv_branch_whitelist.
# Whitelist pattern for remotes (set to None to use local branches only)
smv_remote_whitelist = r"^origin$"
# Pattern for released versions
smv_released_pattern = r'^tags/.*$'
# Format for versioned output directories inside the build directory
smv_outputdir_format = '{ref.name}'

# -- Options for HTML output ----------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = 'sphinx_scylladb_theme'

# Theme options are theme-specific and customize the look and feel of a theme
# further.  For a list of options available for each theme, see the
# documentation.
#
html_theme_options = {
    'conf_py_path': 'docs/',
    'default_branch': 'master',
    'github_repository': 'scylladb/scylla',
    'github_issues_repository': 'scylladb/scylla',
    'hide_edit_this_page_button': 'false',
    'hide_version_dropdown': ['master'],
}

# If not None, a 'Last updated on:' timestamp is inserted at every page
# bottom, using the given strftime format.
# The empty string is equivalent to '%b %d, %Y'.
#
html_last_updated_fmt = '%d %B %Y'

# Custom sidebar templates, maps document names to template names.
#
html_sidebars = {'**': ['side-nav.html']}

# Output file base name for HTML help builder.
htmlhelp_basename = 'ScyllaDocumentationdoc'

# URL which points to the root of the HTML documentation. 
html_baseurl = 'https://scylla.docs.scylladb.com'

# Dictionary of values to pass into the template engine’s context for all pages
html_context = {'html_baseurl': html_baseurl}


class AssemblyScriptLexer(JavascriptLexer):
    pass

# Setup Sphinx
def setup(sphinx):
    # Add Markdown support
    sphinx.add_config_value('recommonmark_config', {
        'enable_eval_rst': True,
        'enable_auto_toc_tree': False,
    }, True)
    sphinx.add_transform(AutoStructify)
    
    # Custom lexers
    sphinx.add_lexer("assemblyscript", AssemblyScriptLexer)

