extern "C" {
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
}
#include <cstdio>
#include <fstream>
#include <iostream>
#include <iterator>
#include "getoptpp.h"
#include "incline_config.h"
#include "incline_def_sharded.h"
#include "incline_dbms.h"
#include "incline_driver_sharded.h"
#include "incline_mgr.h"
#ifdef WIN32
#  include "incline_win32.h"
#endif

using namespace std;

static getoptpp::opt_str opt_mode('m', "mode", false, "mode", "standalone");
static getoptpp::opt_str opt_source('s', "source", true, "definition file");
static getoptpp::opt_flag opt_print_only(0, "print-only",
					 "print the SQLs to be issued instead");

static getoptpp::opt_str opt_forwarder_log_file(0, "forwarder-log-file", false,
						"", "");

static auto_ptr<incline_mgr> mgr;

static void run_all_stmt(incline_dbms* dbh, const vector<string>& stmt)
{
  for (vector<string>::const_iterator si = stmt.begin();
       si != stmt.end();
       ++si) {
    if (*opt_print_only) {
      cout << *si << endl;
    } else {
      dbh->execute(*si);
    }
  }
}

inline incline_driver_async_qtable* aq_driver()
{
  if (dynamic_cast<incline_driver_async_qtable*>(mgr->driver()) == NULL) {
    cerr
      << "command only supported under following mode(s): queue-table, shard"
      << endl;
    exit(1);
  }
  return static_cast<incline_driver_async_qtable*>(mgr->driver());
}

inline incline_driver_sharded* shard_driver()
{
  if (dynamic_cast<incline_driver_sharded*>(mgr->driver()) == NULL) {
    cerr
      << "command only supported under following mode(s): shard"
      << endl;
    exit(1);
  }
  return static_cast<incline_driver_sharded*>(mgr->driver());
}

static incline_dbms* dbh()
{
  static auto_ptr<incline_dbms> h;
  if (h.get() == NULL) {
    h.reset(incline_dbms::factory_->create());
  }
  return h.get();
}

static void shutdown_forwarder(int signum)
{
  cerr << "recevied signal:" << signum << ", shutting down..." << endl;
  aq_driver()->should_exit_loop(true);
}

int
main(int argc, char** argv)
{
  string command;
  
  // parse command
  getoptpp::opt_version opt_version('v', "version", VERSION);
  getoptpp::opt_help
    opt_help('h', "help", argv[0],
	     "command\n\n"
	     "Commands:\n"
	     "    create-trigger                installs necessary triggers\n"
	     "    drop-trigger                  unistalls triggers\n"
	     "    create-queue                  creates queue tables\n"
	     "    drop-queue                    drops queue tables\n"
	     "    forward                       runs the forwarder\n");
  if (! getoptpp::getopt(argc, argv)) {
    exit(1);
  }
  argc -= optind;
  argv += optind;
  if (argc == 0) {
    cerr << "no command" << endl;
    exit(1);
  }
  command = *argv++;
  argc--;
  
  { // create manager
    incline_driver* driver;
    if (*opt_mode == "standalone") {
      driver = new incline_driver_standalone();
    } else if (*opt_mode == "queue-table") {
      driver = new incline_driver_async_qtable();
    } else if (*opt_mode == "shard") {
      driver = new incline_driver_sharded();
    } else {
      cerr << "unknown mode:" << *opt_mode << endl;
      exit(1);
    }
    mgr.reset(new incline_mgr(driver));
  }
  
  { // parse source
    picojson::value defs;
    string err;
    if (*opt_source == "-") {
      err = picojson::parse(defs, cin);
    } else {
      ifstream fin;
      fin.open(opt_source->c_str(), ios::in);
      if (! fin.is_open()) {
	cerr << "failed to open file:" << *opt_source << endl;
	exit(2);
      }
      err = picojson::parse(defs, fin);
      fin.close();
    }
    if (err.empty()) {
      err = mgr->parse(defs);
    }
    if (! err.empty()) {
      cerr << "failed to parse file:" << *opt_source << ": " << err << endl;
      exit(3);
    }
  }
  
  // connect to database
  if (! incline_dbms::setup_factory()) {
    cerr << "rdbms:" << *incline_dbms::opt_rdbms_ << " is not supported"
	 << endl;
    exit(1);
  }
  
  // parse sharded_source
  if (*opt_mode == "shard") {
    pair<string, unsigned short>
      hostport(incline_dbms::factory_->get_hostport());
    string err = shard_driver()->init(hostport.first, hostport.second);
    if (! err.empty()) {
      cerr << err << endl;
      exit(3);
    }
  }
  
  // handle the command
  if (command == "create-trigger") {
    vector<string> stmt(mgr->create_trigger_all(false));
    run_all_stmt(dbh(), stmt);
  } else if (command == "drop-trigger") {
    vector<string> stmt(mgr->drop_trigger_all(true));
    run_all_stmt(dbh(), stmt);
  } else if (command == "create-queue") {
    vector<string> stmt(aq_driver()->create_table_all(false, dbh()));
    run_all_stmt(dbh(), stmt);
  } else if (command == "drop-queue") {
    vector<string> stmt(aq_driver()->drop_table_all(true));
    run_all_stmt(dbh(), stmt);
  } else if (command == "forward") {
    FILE* log_fh = NULL;
    if (*opt_forwarder_log_file == "-") {
      log_fh = stdout;
    } else if (! opt_forwarder_log_file->empty()) {
      if ((log_fh = fopen(opt_forwarder_log_file->c_str(), "w+")) == NULL) {
	fprintf(stderr, "failed to open log file:%s:%s\n",
		opt_forwarder_log_file->c_str(), strerror(errno));
	exit(3);
      }
    }
#ifdef WIN32
#else
    signal(SIGHUP, shutdown_forwarder);
#endif
    signal(SIGTERM, shutdown_forwarder);
    aq_driver()->run_forwarder(1, log_fh);
    // TODO close log_fh
  } else {
    fprintf(stderr, "unknown command: %s\n", command.c_str());
    exit(1);
  }
  
  return 0;
}
