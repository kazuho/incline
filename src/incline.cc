extern "C" {
#include <fcntl.h>
}
#include <fstream>
#include <iostream>
#include <iterator>
#include "getoptpp.h"
#include "start_thread.h"
#include "incline_config.h"
#include "incline.h"

using namespace std;

static getoptpp::opt_str opt_mode('m', "mode", false, "mode", "standalone");
static getoptpp::opt_str opt_source('s', "source", true, "definition file");

static getoptpp::opt_str opt_forwarder_log_file(0, "forwarder-log-file", false,
						"", "");

static getoptpp::opt_str opt_shard_source('S', "shard-source", false,
					  "shard definition file", "");

static incline_mgr* mgr = NULL;

static void run_all_stmt(incline_dbms* dbh, const vector<string>& stmt)
{
  for (vector<string>::const_iterator si = stmt.begin();
       si != stmt.end();
       ++si) {
    dbh->execute(*si);
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
  static incline_dbms* dbh = NULL;
  if (dbh == NULL) {
    dbh = incline_dbms::factory_->create();
  }
  return dbh;
}

int
main(int argc, char** argv)
{
  string command;
  
  // parse command
  getoptpp::opt_version opt_version('v', "version", VERSION);
  getoptpp::opt_help opt_help('h', "help", argv[0], "load-triggers");
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
    mgr = new incline_mgr(driver);
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
    if (opt_shard_source->empty()) {
      cerr << "no --shard-source" << endl;
      exit(1);
    }
    string err = shard_driver()->parse_shard_def(*opt_shard_source);
    if (! err.empty()) {
      cerr << err << endl;
      exit(3);
    }
    pair<string, unsigned short>
      hostport(incline_dbms::factory_->get_hostport());
    err = shard_driver()->set_hostport(hostport.first, hostport.second);
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
  } else if (command == "print-trigger") {
    vector<string> stmt(mgr->create_trigger_all(false));
    picojson::value a(picojson::array_type, false);
    copy(stmt.begin(), stmt.end(), back_inserter(a.get<picojson::array>()));
    a.serialize(ostream_iterator<char>(cout));
    cout << endl;
  } else if (command == "create-queue") {
    vector<string> stmt(aq_driver()->create_table_all(false, dbh()));
    run_all_stmt(dbh(), stmt);
  } else if (command == "drop-queue") {
    vector<string> stmt(aq_driver()->drop_table_all(true));
    run_all_stmt(dbh(), stmt);
  } else if (command == "forward") {
    int log_fd = -1;
    if (*opt_forwarder_log_file == "-") {
      log_fd = 1;
    } else if (! opt_forwarder_log_file->empty()) {
      log_fd = open(opt_forwarder_log_file->c_str(),
		    O_WRONLY | O_APPEND | O_CREAT, 0666);
      if (log_fd == -1) {
	fprintf(stderr, "failed to open log file:%s\n",
		opt_forwarder_log_file->c_str());
	exit(3);
      }
    }
    incline_driver_async_qtable::forwarder_mgr* mgr
      = aq_driver()->create_forwarder_mgr(1, log_fd);
    mgr->run();
    delete mgr;
  } else {
    fprintf(stderr, "unknown command: %s\n", command.c_str());
    exit(1);
  }
  
  return 0;
}
