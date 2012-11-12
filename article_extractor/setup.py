#!/usr/bin/env python
 
from distutils.core import setup
from distutils.extension import Extension

GLIB_PATH = '../../glib'

setup(name="PackageName",
		ext_modules=[
		Extension(
			"article_extractor",
			sources = ["article_extractor.cpp"],
			include_dirs = ['/usr/local/include', GLIB_PATH, '.'],
			libraries = ['rt','uuid'],
			define_macros=[('SIZE_MAX','UINT_MAX'), ('NDEBUG',None)],
			extra_objects = [GLIB_PATH+'/base.o', GLIB_PATH+'/mine.o', GLIB_PATH+'/htmlclean.o',],
			extra_link_args = ['-lrt','-luuid']  # for tracing, add '--coverage', '-pg'
			)
		])
