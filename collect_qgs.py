#!/usr/bin/env python
# -*- coding: utf-8 -*-
## python collect_qgs.py
#import os
#import sys
#os.environ.setdefault("DJANGO_SETTINGS_MODULE", "dj_cms.settings")
#import dj_cms.settings
#from django.db.models import Q
#import mc.local

from mc.fnc_collecter import load_qgs_cnt, chck_qgs_cnt, chck_dts, chck_qgs_dts_upd
from mc.models import sys_def

print 'Hello'

#wip_dir = 'J:\\GIS\\qgis_srv\\'

def load_qgs_wip(plt='NoneDef'):
	try:
		is_gis_dir_obj = sys_def.objects.get(sys_def_var='is_gis_dir')
		is_gis_dir = is_gis_dir_obj.sys_def_val
	except sys_def.DoesNotExist:
		is_gis_dir = 'Semmi'
		print '\nis_gis_dir NOT NOT NOT: %s' % is_gis_dir

	try:
		gis_home_obj = sys_def.objects.get(sys_def_var='gis_home')
		wip_dir = gis_home_obj.sys_def_val.replace('/', '\\')
	except sys_def.DoesNotExist:
		wip_dir = 'Semmi'
		print '\nwip_dir NOT NOT NOT: %s' % wip_dir

	if is_gis_dir == '-1':
		print '\nwip_dir: %s' % wip_dir
		load_qgs_cnt(wip_dir, plt)
	else:
		print '\nwip_dir NOT: %s' % wip_dir

plt = 'discover'

load_qgs_wip(plt)

print '\nBello'
