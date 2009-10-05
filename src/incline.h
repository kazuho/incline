#ifndef incline_h
#define incline_h

#define _INCLINE_TO_S(n) #n
#define INCLINE_TO_S(n) _INCLINE_TO_S(n)

#define INCLINE_VERSION 0.01
#define INCLINE_VERSION_STR INCLINE_TO_S(INCLINE_VERSION)

#include "incline_def.h"
#include "incline_def_async.h"
#include "incline_def_async_qtable.h"
#include "incline_def_sharded.h"
#include "incline_dbms.h"
#include "incline_driver.h"
#include "incline_driver_async.h"
#include "incline_driver_async_qtable.h"
#include "incline_driver_sharded.h"
#include "incline_mgr.h"
#include "incline_util.h"
#include "picojson.h"

#endif
