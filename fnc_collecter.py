#!/usr/bin/env python
# -*- coding: utf-8 -*-
## python fnc_collecter.py
import os, sys, time, re
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "dj_cms.settings")
import dj_cms.settings
from django.db.models import Q
import mc.local
from datetime import datetime, timedelta
from mc.models import dts_cat, qgs_cat, qgs_src, qgs_grp, qgs_tre, tre_map, qgs_xrf
from mc.models import qgs_lyr, qgs_sht, qgs_cmp, qgs_vis, dts_log
from mc.models import qgs_svg, lyr_svg, qgs_ttf, lyr_ttf
from mc.geo import load_qgs_meta, load_dts_cat #, dts_log_add
import subprocess
import shutil

qgs_ext = ['qgs']
tile_a = 256
tsize1 = 20037508.342789244; # (R * math.pi)
rst_ext = ['img','tif','jpg','png']
def coll_vrt(wip_dir):
	wip_dir = os.path.normpath(wip_dir)
	clock = 0
	for root, dirs, pfiles in os.walk(wip_dir, topdown=True):
		tlist = []
		vlist = []
		for dir in dirs:
			for sroot, sdirs, files in os.walk(os.path.join(wip_dir, dir), topdown=True):
				list = []
				for name in files:
					clock += 1
					l = os.path.join(sroot, name)
					(root_splt, ext) =  os.path.splitext(l)
					ext = ext.replace('.', '')
					if ext in rst_ext:
						ln = ' '.join(('\rApndng ...', name ))
						sys.stdout.write("%s - %s" % (mc.local.clock_ar_up[clock % 8], '{:<75}'.format(ln)))
						list.append(os.path.join(sroot, name))
						zoom = int(re.sub(r'(\d+)-(\d+)-(\d+).*$', r'\1', name))
						xtile = int(re.sub(r'(\d+)-(\d+)-(\d+).*$', r'\2', name))
						ytile = int(re.sub(r'(\d+)-(\d+)-(\d+).*$', r'\3', name))
				p = ' '.join(str(x) for x in list)
				p = p.replace('\\', '/')
				vrt = ''.join((str(sroot).replace('\\', '/'), '.vrt'))
				tif = ''.join((str(sroot).replace('\\', '/'), '.tif'))
				cmd = ' '.join(('gdalbuildvrt', vrt, p))
				#print cmd
				r = subprocess.check_output(cmd)
				#print(r)
				cmd = ' '.join(('gdal_translate -of GTiff -co "TILED=YES"', vrt, tif))
				#print cmd
				r = subprocess.check_output(cmd)
				#print(r)
				tlist.append(tif)
				vlist.append(vrt)
			p = ' '.join(str(x) for x in tlist)
			p = p.replace('\\', '/')
			vrt = ''.join((str(root).replace('\\', '/'), '.vrt'))
			tif = ''.join((str(root).replace('\\', '/'), '.tif'))
			tif_rgb = ''.join((str(root).replace('\\', '/'), '_rgb.tif'))
			cmd = ' '.join(('gdalbuildvrt', vrt, p))
			print cmd
			#r = subprocess.check_output(cmd)
			#print(r)
			cmd = ' '.join(('gdal_translate -of GTiff -co "TILED=YES"', vrt, tif))
			print cmd
		for v in vlist:
			dst = v.replace(':/', ':/31/')
			if os.path.exists(os.path.dirname(dst)) == False:
				os.makedirs(os.path.dirname(dst))
			shutil.move(v, dst)

def load_qgs_cnt(wip_dir, plt='not_def', force=False, fin=200):
	upd_host = 'no'
	wip_dir = os.path.normpath(wip_dir)
	list = []
	if os.path.isfile(wip_dir) or force:
		try:
			is_qgs = qgs_cat.objects.get(fn=wip_dir)
			if is_qgs.result == 2:
				list.append(wip_dir)
				is_qgs.result = 3
				is_qgs.save()
				upd_host = 'yes'
				upd_host_id = is_qgs.id
				print ('upd_host: %s') % upd_host
		except qgs_cat.DoesNotExist:
			list.append(wip_dir)
			print ('append: %s') % wip_dir
	else:
		clock = 0
		for root, dirs, files in os.walk(wip_dir, topdown=True):
			print ('walk: %s') % root
			for name in files:
				clock += 1
				for ext in qgs_ext:
					if name.endswith(ext):
						l = os.path.normpath(os.path.join(root, name))
						ln = ''.join(('Appending ... ' , l)).encode('ascii', 'replace')
						try:
							is_cat = qgs_cat.objects.get(fn=l)
							if is_cat.result < 200 and is_cat.result <> 3:
								sys.stdout.write("\r%s - %s" % (mc.local.clock_ar_up[clock % 8], '{:<75}'.format(ln)))
								list.append(l)
						except qgs_cat.DoesNotExist:
							sys.stdout.write("\r%s - %s" % (mc.local.clock_ar_up[clock % 8], '{:<75}'.format(ln)))
							list.append(l)
	clock = 0
	for fn in list:
		clock += 1
		ln = ''.join(('Listing ... ' , fn)).encode('ascii', 'replace')
		sys.stdout.write("\r%s - %s" % (mc.local.clock_ar_up[clock % 8], '{:<75}'.format(ln)))
		(treez,vizer,srid,compozer,lyrz,embddd,uu_id,crt_tim,mod_tim,stat) = load_qgs_meta(fn)
		is_qgs, created = qgs_cat.objects.get_or_create(fn=fn,
				  defaults={'srid':srid,'uuid':uu_id,'crt_tim':crt_tim,\
				  'mod_tim':mod_tim,'status':stat,'tbl_130xid':mc.local.tbl_130xid})
		upd_host_id = is_qgs.id
		host_id = is_qgs.id
		is_qgs.result = 3
		is_qgs.save()
		upd_host = 'yes'
		ln = ''.join(('INS: ' , fn)).encode('ascii', 'replace')
		sys.stdout.write("\r%s - %s" % (mc.local.clock_ar_up[clock % 8], '{:<75}'.format(ln)))

		for map_lyr in lyrz:
			clock += 1
			fn_lyrz = map_lyr["datasource"]
			load_dts_cat(fn_lyrz, plt, True)
			is_dts = dts_cat.objects.get(fn__iexact=fn_lyrz)
			is_dts_lyr, created = qgs_src.objects.get_or_create(qgs_id=host_id,dts_id=is_dts.id)
			map_src_id = is_dts_lyr.id
			(maplyr_id,lyrname,title,abstr,minscl,maxscl,\
				sclvisflg,minlblscl,maxlblscl,scllblflg,data_typ,geom_typ,provdr,\
				srid,sql_str,svgz,ttfz,diag,copyr) = map_lyr["map_meta"]
			print map_lyr["map_meta"]
			crit1 = Q(qgs_id=host_id)
			crit2 = Q(src_id=map_src_id)
			crit3 = Q(maplyr_id=maplyr_id)
			print 'lyrname: %s' % lyrname
			if lyrname is not None and len(lyrname) > 255:
#				lyrname = ''.join((lyrname[:250] , '...'))
				print 'lyrname: %s' % lyrname
			if abstr is not None and len(abstr) > 255:
#				abstr = ''.join((abstr[:250] , '...'))
				print 'abstr: %s' % abstr
			if sql_str is not None and len(sql_str) > 255:
#				sql_str = ''.join((sql_str[:250] , '...'))
				print 'sql_str: %s' % sql_str
			is_lyr, created = qgs_lyr.objects.get_or_create(qgs_id=host_id,src_id=map_src_id,maplyr_id=maplyr_id,
					  defaults={'lyrname':lyrname,'title':title,'abstr':abstr,'minscl':minscl,'maxscl':maxscl,
					  'sclvisflg':sclvisflg,'minlblscl':minlblscl,'maxlblscl':maxlblscl,'scllblflg':scllblflg,
					  'data_typ':data_typ,'geom_typ':geom_typ,'provdr':provdr,'srid':srid,'sql_str':sql_str,
					  'diag':diag})
			is_lyr_id = is_lyr.id
			for svg in svgz:
				is_svg, created = qgs_svg.objects.get_or_create(fn=svg)
				is_lyr_svg, created = lyr_svg.objects.get_or_create(qgs_id=host_id,lyr_id=is_lyr_id,svg_id=is_svg.id)
			for ttf in ttfz:
				is_ttf, created = qgs_ttf.objects.get_or_create(fn=ttf)
				is_lyr_ttf, created = lyr_ttf.objects.get_or_create(qgs_id=host_id,lyr_id=is_lyr_id,ttf_id=is_ttf.id)
		for emb_lyr in embddd:
			clock += 1
			fn_emb = emb_lyr
			load_qgs_cnt(fn_emb, plt, True, fin)
			print 'fn_emb: %s' % fn_emb
			is_emb = qgs_cat.objects.get(fn__iexact=fn_emb)
			xrf_obj, created = qgs_xrf.objects.get_or_create(hst_id=host_id,xrf_id=is_emb.id)

		for cmp_ozr in compozer:
			title = cmp_ozr['title']
			scl = cmp_ozr['scl']
			ext_ymi = cmp_ozr['ext_ymi']
			ext_xmi = cmp_ozr['ext_xmi']
			ext_yma = cmp_ozr['ext_yma']
			ext_xma = cmp_ozr['ext_xma']
			sht_h = cmp_ozr['sht_h']
			sht_w = cmp_ozr['sht_w']
			wld = cmp_ozr['wld']
			atl = cmp_ozr['atl']
			is_sht, created = qgs_sht.objects.get_or_create(qgs_id=host_id,title=title,
				defaults={'ext_ymi':ext_ymi,'ext_xmi':ext_xmi,'ext_yma':ext_yma,'ext_xma':ext_xma,\
				'sht_w':sht_w,'sht_h':sht_h,'wld':wld,'scl':scl,'atl':atl})
			for cmp_lyr in cmp_ozr['lyrz']:
				clock += 1
				is_cmp, created = qgs_cmp.objects.get_or_create(qgs_id=host_id,sht_id=is_sht.id,lyr_id=cmp_lyr,
					defaults={'status':'A'})
				ln = ''.join((str(created),': ' , cmp_lyr)).encode('ascii', 'replace')
				sys.stdout.write("\r%s - %s" % (mc.local.clock_ar_up[clock % 8], '{:<75}'.format(ln)))

		for vis_pre in vizer:
			vis_name = vis_pre['vis_name']
			for vis_lyr in vis_pre['lyrz']:
				clock += 1
				is_vis, created = qgs_vis.objects.get_or_create(qgs_id=host_id,vis_name=vis_name,lyr_id=vis_lyr,
					defaults={'status':'A'})
				ln = ''.join((str(created),': ' , vis_lyr)).encode('ascii', 'replace')
				sys.stdout.write("\r%s - %s" % (mc.local.clock_ar_up[clock % 8], '{:<75}'.format(ln)))

		for lyr_tre in treez:
			grp_name = lyr_tre['grp_name']
			clock += 1
			if len(lyr_tre['fn']) > 0:
				ln = ''.join(('INS: ' , grp_name)).encode('ascii', 'replace')
				sys.stdout.write("\r%s - %s" % (mc.local.clock_ar_up[clock % 8], '{:<75}'.format(ln)))
				get_qgs, created = qgs_cat.objects.get_or_create(fn=lyr_tre['fn'])
				try:
					is_grp_prnt = qgs_grp.objects.get(qgs_id=get_qgs.id,grp_name=lyr_tre['grp_name'])
					is_grp, created = qgs_grp.objects.get_or_create(qgs_id=host_id,grp_name=lyr_tre['grp_name'],
					grp_id=is_grp_prnt.id,defaults={'chckd':lyr_tre['chckd']})
				except:
					print ' Bububu is_grp fn'
			else:
				if lyr_tre['prnt_name'] is None or len(lyr_tre['prnt_name']) == 0:
					is_grp, created = qgs_grp.objects.get_or_create(qgs_id=host_id,grp_name=lyr_tre['grp_name'],
						defaults={'chckd':lyr_tre['chckd']})
				else:
					is_grp_prnt = qgs_grp.objects.get(qgs_id=host_id,grp_name=lyr_tre['prnt_name'])
					is_grp, created = qgs_grp.objects.get_or_create(qgs_id=host_id,grp_name=lyr_tre['grp_name'],
						grp_id=is_grp_prnt.id,defaults={'chckd':lyr_tre['chckd']})
				for grp_lyr in lyr_tre['lyrz']:
					chckd,lyr_id,name = grp_lyr
					clock += 1
					is_tre, created = qgs_tre.objects.get_or_create(qgs_id=host_id,grp_id=is_grp.id,lyr_id=lyr_id,
						defaults={'lyrname':name,'chckd':chckd})
					ln = ''.join((str(created),': ' , lyr_id)).encode('ascii', 'replace')
					sys.stdout.write("\r%s - %s" % (mc.local.clock_ar_up[clock % 8], '{:<75}'.format(ln)))

		if upd_host == 'yes':
			upd_qgs = qgs_cat.objects.get(id=upd_host_id)
			upd_qgs.result = fin
			upd_qgs.save()
			upd_host = 'done'
	return

def chck_qgs_cnt(rslt=0, plt='not_def', force=False, fin=200):
	print 'chck_qgs_cnt rslt  %s fin: %s' % (rslt,  fin)
	if rslt == 2:
		chck_qgs_add(rslt, plt, force, fin)
		return
	if rslt == 22:
		chck_qgs_check_1day(rslt, plt, force, fin)
		return
	if rslt == 551:
		chck_qgs_update(rslt, plt, force, fin)
		return
	if rslt == 6161:
		chck_qgs_replace(rslt, plt, force, fin)
	return

def chck_qgs_check_1day(rslt=0, plt='not_def', force=False, fin=200):
	clock = 0
	if rslt == 22:
		now_wip = datetime.now()
		td = time.time() - 60*60*24
		dt = datetime.fromtimestamp(td)
		try:
			is_qgs_cat = qgs_cat.objects.filter(result=200)
			for is_qgs in is_qgs_cat:
				st = os.stat(is_qgs.fn)
				t = datetime.fromtimestamp(st.st_mtime)
				clock += 1
				ln = ''.join(('Load: ' , is_qgs.fn)).encode('ascii', 'replace')
				sys.stdout.write("\r%s - %s" % (mc.local.clock_ar_up[clock % 8], '{:<75}'.format(ln)))
				print 't: %s' % t
				print 'dt: %s' % dt
				if t > dt:
#					is_qgs.result = 2
					is_qgs.result = 551
					is_qgs.save()
			chck_qgs_cnt(2, plt, True, 551)
			chck_qgs_cnt(551, plt, True, 6161)
			chck_qgs_cnt(6161, plt, True, 22)
#			chck_qgs_cnt(551, plt, True, 22)
		except qgs_cat.DoesNotExist:
			print 'Bububu result=200'
			return
	else:
		return
	return

def chck_qgs_replace(rslt=0, plt='not_def', force=False, fin=200):
	print 'chck_qgs_replace rslt  %s fin: %s' % (rslt,  fin)
	clock = 0
	if rslt == 6161:
		try:
			is_qgs_cat = qgs_cat.objects.filter(result=rslt)
		except qgs_cat.DoesNotExist:
			print 'Bububu'
			return
	else:
		return

	for host_qgs in is_qgs_cat:
		host_qgs.result = 3
		host_qgs.save()
		host_id = host_qgs.id
		clock += 1
		ln = ''.join(('Load: ' , host_qgs.fn)).encode('ascii', 'replace')
		sys.stdout.write("\r%s - %s" % (mc.local.clock_ar_up[clock % 8], '{:<75}'.format(ln)))
		with open(host_qgs.fn, 'r') as file_content:
			clock += 1
			ln = ''.join(('Read: ' , host_qgs.fn)).encode('ascii', 'replace')
			sys.stdout.write("\r%s - %s" % (mc.local.clock_ar_up[clock % 8], '{:<75}'.format(ln)))
			content = file_content.read()
		q1 = ~Q(result=200)
#		q2 = Q(qgs_id=host_id)
		q3 = Q(lyr_id_a__isnull=False)
		q4 = Q(result__lt=999)
		try:
			is_tre_map = tre_map.objects.filter(q1 & q3 & q4)
			for is_map in is_tre_map:
				clock += 1
				ln = ''.join(('Replace: ' , is_map.lyr_id_d)).encode('ascii', 'replace')
				sys.stdout.write("\r%s - %s" % (mc.local.clock_ar_up[clock % 8], '{:<75}'.format(ln)))
				content = content.decode("utf-8").replace(is_map.lyr_id_d, is_map.lyr_id_a).encode("utf-8")
			clock += 1
			ln = ''.join(('Write: ' , host_qgs.fn)).encode('ascii', 'replace')
			sys.stdout.write("\r%s - %s" % (mc.local.clock_ar_up[clock % 8], '{:<75}'.format(ln)))
			with open(host_qgs.fn, "w") as text_file:
				text_file.write(content)
		except tre_map.DoesNotExist:
			print 'Bububu tre_map %s' % '184'
			return
		host_qgs.result = fin
		host_qgs.save()
	return

def chck_qgs_dts_upd(rslt=0, plt='not_def', force=False, fin=200):
	print 'chck_dts_cat_upd %s fin: %s' % (rslt,  fin)
	clock = 0
	q1 = Q(status='c')
	try:
		is_dts_cat = dts_cat.objects.filter(q1)
	except dts_cat.DoesNotExist:
		print 'Bububu dts_cat'
		return
	for dts_cat_chck in is_dts_cat:
		q2 = Q(dts_id=dts_cat_chck.id)
		try:
			is_qgs_src = qgs_src.objects.filter(q2)
		except qgs_src.DoesNotExist:
			print 'Bububu qgs_src'
			return
		try:
			is_dts_log = dts_log.objects.get(q1 & q2)
		except qgs_src.DoesNotExist:
			print 'Bububu dts_cat_log'
			return
		for qgs_src_chck in is_qgs_src:
			try:
				is_qgs_cat = qgs_cat.objects.filter(id=qgs_src_chck.qgs_id)
			except qgs_cat.DoesNotExist:
				print 'Bububu qgs_cat'
				return
			for host_qgs in is_qgs_cat:
				clock += 1
				ln = ''.join(('Load: ' , host_qgs.fn)).encode('ascii', 'replace')
				sys.stdout.write(" %s - %s" % (mc.local.clock_ar_up[clock % 8], '{:<75}'.format(ln)))
				with open(host_qgs.fn, 'r') as file_content:
					content = file_content.read()
					content = content.decode("utf-8").replace(is_dts_log.fn, dts_cat_chck.fn).encode("utf-8")
					clock += 1
					ln = ''.join(('Write: ' , host_qgs.fn)).encode('ascii', 'replace')
					sys.stdout.write("\r%s - %s" % (mc.local.clock_ar_up[clock % 8], '{:<75}'.format(ln)))
					with open(host_qgs.fn, "w") as text_file:
						text_file.write(content)
		is_dts_log.status = 'Z'
		is_dts_log.save()
		dts_cat_chck.status = 'A'
		dts_cat_chck.save()
	return

def chck_qgs_add(rslt=0, plt='not_def', force=False, fin=200):
	print 'chck_qgs_add rslt  %s fin: %s' % (rslt,  fin)
	if rslt == 2:
		try:
			is_qgs_cat = qgs_cat.objects.filter(result=rslt)
			for is_qgs in is_qgs_cat:
				load_qgs_cnt(is_qgs.fn, plt, force, fin)
		except qgs_cat.DoesNotExist:
			print 'Bububu'
			return
		return
	return

def chck_qgs_update(rslt=0, plt='not_def', force=False, fin=200):
	print 'chck_qgs_update rslt  %s fin: %s' % (rslt,  fin)
	crit_stat_d = ~Q(status='D')
	crit_stat_a = Q(status='A')
	clock = 0
	try:
		is_qgs = qgs_cat.objects.filter(result=rslt)
	except qgs_cat.DoesNotExist:
		print 'Bububu'
		return
	for host_qgs in is_qgs:
		host_id = host_qgs.id
		crit_host_id = Q(qgs_id=host_id)
		print 'qgs_cat id: %s' % host_id
		(treez,vizer,srid,compozer,lyrz,embddd,uu_id,crt_tim,mod_tim,stat) = load_qgs_meta(host_qgs.fn)
		embddd_ar = {}
		for emb_lyr in embddd:
			fn_emb = emb_lyr
			try:
				is_emb = qgs_cat.objects.get(fn__iexact=fn_emb)
				embddd_ar[''.join(('emb_id_', str(is_emb.id)))] = is_emb.id
			except qgs_cat.DoesNotExist:
				print 'Bububu fn__iexact=fn_emb'
		criter_hst_id = Q(hst_id=host_id)
		try:
			is_xrf = qgs_xrf.objects.filter(criter_hst_id & crit_stat_d)
		except qgs_xrf.DoesNotExist:
			print 'Bububu qgs_xrf hst_id=host_id'
		for xrf_qgs in is_xrf:
			key = ''.join(('emb_id_', str(xrf_qgs.xrf_id)))
			try:
				txt = embddd_ar[key]
			except:
				print 'deleted xref: %s' % xrf_qgs.xrf_id
				del_xrf = qgs_xrf.objects.get(id=xrf_qgs.id)
				del_xrf.status = 'D'
				del_xrf.save()
		data_src_ar = {}
		maplyr_ar = {}
		for map_lyr in lyrz:
			fn_lyrz = map_lyr["datasource"]
			try:
				is_dts = dts_cat.objects.get(fn__iexact=fn_lyrz)
				is_dts.status = map_lyr["dts_stat"]
				is_dts.save()
			except dts_cat.DoesNotExist:
				print 'Bububu dts_cat fn__iexact=fn_lyrz'
			crit_dts_id = Q(dts_id=is_dts.id)
			try:
				is_dts_lyr = qgs_src.objects.get(crit_host_id & crit_dts_id)
				data_src_ar[''.join(('src_id_', str(is_dts_lyr.id)))] = is_dts_lyr.id
			except qgs_src.DoesNotExist:
				print 'Bububu qgs_src dts crit_host_id & crit_dts_id'
			(maplyr_id,lyrname,title,abstr,minscl,maxscl,\
			sclvisflg,minlblscl,maxlblscl,scllblflg,data_typ,geom_typ,provdr,\
			srid,sql_str,svgz,ttfz,diag,copyr) = map_lyr["map_meta"]
			crit_maplyr_id = Q(maplyr_id=maplyr_id)
			crit_src_id = Q(src_id=is_dts_lyr.id)
			try:
				is_qgs_lyr = qgs_lyr.objects.get(crit_host_id & crit_maplyr_id & crit_src_id)
				maplyr_ar[''.join(('maplyr_id_', str(is_qgs_lyr.id)))] = map_lyr["map_meta"]+(is_dts_lyr.id,)
			except qgs_lyr.DoesNotExist:
				print 'Bububu qgs_lyr crit_host_id & crit_maplyr_id'
		try:
			is_src = qgs_src.objects.filter(crit_host_id & crit_stat_d)
		except qgs_src.DoesNotExist:
			print 'Bububu qgs_src qgs_id=host_id'
		for src_qgs in is_src:
			key = ''.join(('src_id_', str(src_qgs.id)))
			try:
				txt = data_src_ar[key]
			except:
				print 'deleted src: %s' % src_qgs.id
				del_src = qgs_src.objects.get(id=src_qgs.id)
				del_src.status = 'D'
				del_src.save()
		try:
			is_lyr = qgs_lyr.objects.filter(crit_host_id)
		except qgs_lyr.DoesNotExist:
			print 'Bububu qgs_lyr crit_host_id & crit_stat_d'
		for lyr_qgs in is_lyr:
			key = ''.join(('maplyr_id_', str(lyr_qgs.id)))
			try:
				(maplyr_id,lyrname,title,abstr,minscl,maxscl,sclvisflg,minlblscl,\
				maxlblscl,scllblflg,data_typ,geom_typ,provdr,srid,sql_str,svgz,ttfz,diag,copyr,src_id)  = maplyr_ar[key]
				crit_maplyr_id = Q(maplyr_id=maplyr_id)
				crit_src_id = Q(src_id=src_id)
				upd_lyr = qgs_lyr.objects.get(crit_host_id & crit_maplyr_id & crit_src_id)
				upd_lyr.lyrname = lyrname
				upd_lyr.title = title
				upd_lyr.abstr = abstr
				upd_lyr.minscl = minscl
				upd_lyr.maxscl = maxscl
				upd_lyr.sclvisflg = sclvisflg
				upd_lyr.minlblscl = minlblscl
				upd_lyr.maxlblscl = maxlblscl
				upd_lyr.scllblflg = scllblflg
				upd_lyr.data_typ = data_typ
				upd_lyr.geom_typ = geom_typ
				upd_lyr.provdr = provdr
				upd_lyr.srid = srid
				upd_lyr.diag = diag
				upd_lyr.sql_str = sql_str
				upd_lyr.status = 'A'
				upd_lyr.save()
			except:
				print 'deleted maplyr: %s' % lyr_qgs.id
				del_lyr = qgs_lyr.objects.get(id=lyr_qgs.id)
				del_lyr.status = 'D'
				del_lyr.save()
		cmpz_ar = {}
		for cmp_sht in compozer:
			title = cmp_sht['title']
			print '\ntitle: %s' % title
			crit_title = Q(title=title)
			try:
				is_sht = qgs_sht.objects.get(crit_host_id & crit_title)
			except qgs_sht.DoesNotExist:
				print ' Bububu qgs_sht crit_host_id & crit_title'
			for cmp_lyr in cmp_sht['lyrz']:
				clock += 1
				crit_sht_id = Q(sht_id=is_sht.id)
				crit_cmp_lyr = Q(lyr_id=cmp_lyr)
				ln = ''.join(('chck: ' , cmp_lyr)).encode('ascii', 'replace')
				sys.stdout.write("\r%s - %s" % (mc.local.clock_ar_up[clock % 8], '{:<75}'.format(ln)))
				try:
					is_cmp = qgs_cmp.objects.get(crit_host_id & crit_sht_id & crit_cmp_lyr)
					cmpz_ar[''.join(('cmp_id_', str(is_cmp.id)))] = is_cmp.id
				except qgs_cmp.DoesNotExist:
					print ' Bububu qgs_cmp crit_host_id & crit_sht_id & crit_cmp_lyr'
		try:
			is_cmp = qgs_cmp.objects.filter(crit_host_id & crit_stat_d)
		except qgs_cmp.DoesNotExist:
			print ' Bububu qgs_cmp qgs_id=host_id'
		print ''
		for cmp_qgs in is_cmp:
			clock += 1
			key = ''.join(('cmp_id_', str(cmp_qgs.id)))
			try:
				txt = cmpz_ar[key]
			except:
				print ' deleted comp: %s' % cmp_qgs.id
				del_cmp = qgs_cmp.objects.get(id=cmp_qgs.id)
				del_cmp.status = 'D'
				del_cmp.save()
				del_cmp2 = qgs_cmp.objects.get(id=cmp_qgs.id)
				print ' deleted del_cmp2.id: %s' % del_cmp2.id
				print ' deleted del_cmp2.status: %s' % del_cmp2.status
			sys.stdout.write("\r%s - %s" % (mc.local.clock_ar_up[clock % 8], '{:<75}'.format('')))

		visz_ar = {}
		for vis_pre in vizer:
			vis_name = vis_pre['vis_name']
			print '\nvis_name: %s' % vis_name
			for vis_lyr in vis_pre['lyrz']:
				clock += 1
				crit_vis_name = Q(vis_name=vis_name)
				crit_vis_lyr = Q(lyr_id=vis_lyr)
				ln = ''.join(('chck: ' , vis_lyr)).encode('ascii', 'replace')
				sys.stdout.write("\r%s - %s" % (mc.local.clock_ar_up[clock % 8], '{:<75}'.format(ln)))
				try:
					is_vis = qgs_vis.objects.get(crit_host_id & crit_vis_name & crit_vis_lyr)
					visz_ar[''.join(('vis_id_', str(is_vis.id)))] = is_vis.id
				except qgs_vis.DoesNotExist:
					print ' Bububu qgs_vis crit_host_id & crit_vis_name & crit_vis_lyr'
		try:
			is_vis = qgs_vis.objects.filter(crit_host_id) #  & crit_stat_d
		except qgs_vis.DoesNotExist:
			print ' Bububu qgs_vis qgs_id=host_id'
		print ''
		for vis_qgs in is_vis:
			clock += 1
			key = ''.join(('vis_id_', str(vis_qgs.id)))
			try:
				txt = visz_ar[key]
				upd_vis = qgs_vis.objects.get(id=vis_qgs.id)
				upd_vis.status = 'A'
				upd_vis.save()
			except:
				del_vis = qgs_vis.objects.get(id=vis_qgs.id)
				del_vis.status = 'D'
				del_vis.save()
			sys.stdout.write("\r%s - %s" % (mc.local.clock_ar_up[clock % 8], '{:<75}'.format('')))

		grpz_ar = {}
		trez_old_ar = {}
		trez_new_ar = {}
		for grp_pre in treez:
			grp_name = grp_pre['grp_name']
			crit_grp_name = Q(grp_name=grp_name)
			if len(grp_pre['fn']) > 0:
				is_cat = qgs_cat.objects.get(fn=grp_pre['fn'])
				crit_is_cat = Q(qgs_id=is_cat.id)
				print ' crit_grp_name: %s' % crit_grp_name
				print ' crit_is_cat: %s' % crit_is_cat
				print ' fn: %s' % grp_pre['fn']
				print ' prnt_name: %s' % grp_pre['prnt_name']
				print ' grp_name: %s' % grp_pre['grp_name']
				try:
					is_prnt = qgs_grp.objects.get(crit_is_cat & crit_grp_name & crit_stat_a)
					crit_is_prnt = Q(grp_id=is_prnt.id)
					ln = ''.join(('chck: ' , grp_pre['fn'])).encode('ascii', 'replace')
					sys.stdout.write("\r%s - %s" % (mc.local.clock_ar_up[clock % 8], '{:<75}'.format(ln)))
					try:
						is_grp = qgs_grp.objects.get(crit_grp_name & crit_host_id & crit_is_prnt)
						grpz_ar[''.join(('grp_id_', str(is_grp.id)))] = grp_pre['chckd']
					except qgs_grp.DoesNotExist:
						print ' Bububu qgs_grp crit_grp_name & crit_host_id & crit_is_prnt'
				except qgs_grp.DoesNotExist:
					print ' Bububu qgs_grp crit_is_cat & crit_grp_name & crit_stat_a'
			else:
				ln = ''.join(('chck: ' , grp_name)).encode('ascii', 'replace')
				sys.stdout.write("\r%s - %s" % (mc.local.clock_ar_up[clock % 8], '{:<75}'.format(ln)))
				if grp_pre['prnt_name'] is None or grp_pre['prnt_name'] == 'Root':
					print ' prnt_name %s' % grp_pre['prnt_name']
					try:
						is_grp = qgs_grp.objects.get(crit_grp_name & crit_host_id & crit_stat_a)
						grpz_ar[''.join(('grp_id_', str(is_grp.id)))] = grp_pre['chckd']
					except qgs_grp.DoesNotExist:
						print ' Bububu qgs_grp crit_grp_name & crit_host_id'
				else:
					crit_prnt_name = Q(grp_name=grp_pre['prnt_name'])
					try:
						is_grp_prnt = qgs_grp.objects.get(crit_prnt_name & crit_host_id)
					except qgs_grp.DoesNotExist:
						print ' Bububu qgs_grp crit_prnt_name & crit_host_id'
					crit_prnt_id = Q(grp_id=is_grp_prnt.id)
					try:
						is_grp = qgs_grp.objects.get(crit_grp_name & crit_host_id & crit_prnt_id)
						grpz_ar[''.join(('grp_id_', str(is_grp.id)))] = grp_pre['chckd']
					except qgs_grp.DoesNotExist:
						print ' Bububu qgs_grp crit_grp_name & crit_host_id & crit_prnt_id'

				crit_grp_id = Q(grp_id=is_grp.id)
				for grp_lyr in grp_pre['lyrz']:
					chckd,lyr_id,name = grp_lyr
					crit_lyr_id = Q(lyr_id=lyr_id)
					ln = ''.join(('chck: ' , lyr_id)).encode('ascii', 'replace')
					sys.stdout.write("\r%s - %s" % (mc.local.clock_ar_up[clock % 8], '{:<75}'.format(ln)))
					try:
						is_tre = qgs_tre.objects.get(crit_host_id & crit_grp_id & crit_lyr_id)
						trez_old_ar[''.join(('tre_id_', str(is_tre.id)))] = {'lyrname':name,'chckd':chckd,'lyr_id':lyr_id}
						trez_new_ar[lyr_id] = name
					except qgs_tre.DoesNotExist:
						print ' Bububu qgs_tre crit_host_id & crit_grp_id & crit_lyr_id'

		try:
			is_grp = qgs_grp.objects.filter(crit_host_id) #  & crit_stat_d
		except qgs_grp.DoesNotExist:
			print ' Bububu qgs_grp qgs_id=host_id'
		print ''
		for grp_qgs in is_grp:
			clock += 1
			key = ''.join(('grp_id_', str(grp_qgs.id)))
			try:
				txt = grpz_ar[key]
				upd_grp = qgs_grp.objects.get(id=grp_qgs.id)
				upd_grp.chckd = txt
				upd_grp.status = 'A'
				upd_grp.save()
			except:
				print ' deleted group: %s' % grp_qgs.id
				del_grp = qgs_grp.objects.get(id=grp_qgs.id)
				del_grp.status = 'D'
				del_grp.save()
			sys.stdout.write("\r%s - %s" % (mc.local.clock_ar_up[clock % 8], '{:<75}'.format('')))
		try:
			is_tre = qgs_tre.objects.filter(crit_host_id) #  & crit_stat_d
		except qgs_tre.DoesNotExist:
			print ' Bububu qgs_tre qgs_id=host_id'
		print ''
		for tre_qgs in is_tre:
			ln = ''
			key = ''.join(('tre_id_', str(tre_qgs.id)))
			try:
				reloc_name = trez_new_ar[tre_qgs.lyr_id]
				# layer relocated not deleted
			except:
				reloc_name = 'AddTreMap'
				# layer deleted (to be replaced)
			try:
				dict = trez_old_ar[key]
				upd_tre = qgs_tre.objects.get(id=tre_qgs.id)
				upd_tre.chckd = dict['chckd']
				upd_tre.lyrname = dict['lyrname']
				upd_tre.status = 'A'
				clock += 1
				ln = ' key %s upd_tre.status: %s' % (key, upd_tre.status)
				sys.stdout.write("\r%s - %s" % (mc.local.clock_ar_up[clock % 8], '{:<75}'.format(ln)))
				upd_tre.save()
			except:
				del_tre = qgs_tre.objects.get(id=tre_qgs.id)
				del_tre.status = 'D'
				clock += 1
				ln = 'del_tre.status: %s' % del_tre.status
				sys.stdout.write("\r%s - %s" % (mc.local.clock_ar_up[clock % 8], '{:<75}'.format(ln)))
				del_tre.save()
				criter_t = Q(lyrname=del_tre.lyrname)
				criter_s = Q(status='A')
				try:
					# new dts with same tre name
					new_tre = qgs_tre.objects.get(crit_host_id & criter_t & criter_s)
					lyr_id_a = new_tre.lyr_id
					tre_map_stat = 'A'
					clock += 1
					ln = 'tre_map_stat: %s' % tre_map_stat
					sys.stdout.write("\r%s - %s" % (mc.local.clock_ar_up[clock % 8], '{:<75}'.format(ln)))
				except:
					criter_tl = Q(lyr_id=del_tre.lyr_id)
					try:
						# same dts with new tre name
						new_tre = qgs_tre.objects.get(crit_host_id & criter_tl & criter_s)
						lyr_id_a = new_tre.lyr_id
#						print ' lyr_id_a: %s' % lyr_id_a
						tre_map_stat = 'A'
						clock += 1
						ln = 'tre_map_stat: %s' % tre_map_stat
						sys.stdout.write("\r%s - %s" % (mc.local.clock_ar_up[clock % 8], '{:<75}'.format(ln)))
					except:
						ln = ' Bububu qgs_tre crit_host_id & criter_tl & criter_s'
						lyr_id_a = None
						tre_map_stat = 'M'
						clock += 1
						ln = 'tre_map_stat: %s' % tre_map_stat
						sys.stdout.write("\r%s - %s" % (mc.local.clock_ar_up[clock % 8], '{:<75}'.format(ln)))
				if del_tre.lyr_id <> lyr_id_a and reloc_name == 'AddTreMap':
					tre_map_obj, created = tre_map.objects.get_or_create(qgs_id=host_id,\
					lyr_id_d=del_tre.lyr_id,defaults={'lyr_id_a':lyr_id_a,'status':tre_map_stat})
			clock += 1
			sys.stdout.write("\r%s - %s" % (mc.local.clock_ar_up[clock % 8], '{:<75}'.format(ln)))
		print ' mod_tim: %s' % mod_tim
		host_qgs.result = fin
		host_qgs.mod_tim = mod_tim
		host_qgs.save()
	return

def chck_dts(rslt=551, plt='not_def', force=False, fin=200):
	clock = 0
	try:
		is_dts = dts_cat.objects.filter(result=rslt)
	except dts_cat.DoesNotExist:
		print 'Bububu result=%s' % rslt
		return
	for dts_row in is_dts:
		clock += 1
		ln = dts_row.fn.encode('ascii', 'replace')
		dts_row.result = 3
		dts_row.save()
		sys.stdout.write("\r%s - %s" % (mc.local.clock_ar_up[clock % 8], '{:<75}'.format(ln)))
		(is_rst_vct,imgw,imgh,geot,proj,srid,uu_id,crt_tim,mod_tim,ext,copyr,stat) = load_dts_cat(ln, plt, force)
		if is_rst_vct == 'list':
			ln = 'N O  F I L E'
			sys.stdout.write("\r%s - %s" % (mc.local.clock_ar_up[clock % 8], '{:<75}'.format(ln)))
