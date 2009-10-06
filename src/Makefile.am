bin_PROGRAMS	= incline
incline_SOURCES	= getoptpp.h incline_dbms.cc incline_dbms.h incline_def.cc incline_def.h incline_def_async.cc incline_def_async.h incline_def_async_qtable.cc incline_def_async_qtable.h incline_def_sharded.cc incline_def_sharded.h incline_driver.cc incline_driver.h incline_driver_async.cc incline_driver_async.h incline_driver_async_qtable.cc incline_driver_async_qtable.h incline_driver_sharded.cc incline_driver_sharded.h incline_driver_standalone.cc incline_driver_standalone.h incline_mgr.cc incline_mgr.h incline_util.cc incline_util.h incline.cc incline.h interthr_call.h picojson.h start_thread.h tmd.h
if WITH_MYSQL
incline_SOURCES	+= incline_mysql.cc incline_mysql.h
endif
if WITH_PGSQL
incline_SOURCES	+= incline_pgsql.cc incline_pgsql.h
endif
EXTRA_DIST	= cac/cac_mutex.h cac/cac_rwlock.h cac/test_mutex.cc cac/test_rwlock.cc
