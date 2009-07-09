#include <fstream>
#include <iostream>
#include <iterator>
#include <sstream>
#include "getoptpp.h"
#include "incline.h"

using namespace std;

static getoptpp::opt_str opt_mode('m', "mode", false, "mode", "standalone");
static getoptpp::opt_str opt_source('s', "source", true, "definition file");
static getoptpp::opt_str opt_rdbms('r', "rdbms", false, "rdbms name", "mysql");
static getoptpp::opt_str opt_database('d', "database", true, "database name");

static getoptpp::opt_str opt_sharded_source('S', "sharded-source", false,
					    "shard definition file", "");

static getoptpp::opt_str opt_mysql_host(0, "mysql-host", false, "mysql host",
					"127.0.0.1");
static getoptpp::opt_str opt_mysql_user(0, "mysql-user", false,
					"mysql user", "root");
static getoptpp::opt_str opt_mysql_password(0, "mysql-password", false,
					    "mysql password", "");
static getoptpp::opt_int opt_mysql_port(0, "mysql-port", false, "mysql port",
					3306);

static incline_mgr* mgr = NULL;

static void run_all_stmt(tmd::conn_t& dbh, const vector<string>& stmt)
{
  for (vector<string>::const_iterator si = stmt.begin();
       si != stmt.end();
       ++si) {
    tmd::execute(dbh, *si);
  }
}

inline incline_driver_async_qtable* aq_driver()
{
  if (dynamic_cast<incline_driver_async_qtable*>(mgr->driver()) == NULL) {
    cerr
      << "command only supported under following mode(s): queue-table, sharded"
      << endl;
    exit(1);
  }
  return static_cast<incline_driver_async_qtable*>(mgr->driver());
}

inline incline_driver_sharded* sharded_driver()
{
  if (dynamic_cast<incline_driver_sharded*>(mgr->driver()) == NULL) {
    cerr
      << "command only supported under following mode(s): sharded"
      << endl;
    exit(1);
  }
  return static_cast<incline_driver_sharded*>(mgr->driver());
}

int
main(int argc, char** argv)
{
  string command;
  
  // parse command
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
    } else if (*opt_mode == "sharded") {
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
  
  // parse sharded_source
  if (*opt_mode == "sharded") {
    picojson::value sharded_def;
    string err;
    if (opt_sharded_source->empty()) {
      cerr << "no --sharded-source" << endl;
      exit(1);
    } else if (*opt_sharded_source == "-") {
      err = picojson::parse(sharded_def, cin);
    } else {
      ifstream fin;
      fin.open(opt_sharded_source->c_str(), ios::in);
      if (! fin.is_open()) {
	cerr << "failed to open file:" << *opt_sharded_source << endl;
	exit(2);
      }
      err = picojson::parse(sharded_def, fin);
      fin.close();
    }
    if (err.empty()) {
      err = sharded_driver()->parse_sharded_def(sharded_def);
    }
    if (! err.empty()) {
      cerr << "failed to parse file:" << *opt_sharded_source << ": " << err
	   << endl;
      exit(3);
    }
    {
      stringstream ss;
      ss << *opt_mysql_host << ':' << *opt_mysql_port;
      err = sharded_driver()->set_hostport(ss.str());
      if (! err.empty()) {
	cerr << err << endl;
	exit(3);
      }
    }
  }
  
  // connect to database
  if (*opt_rdbms != "mysql") {
    cerr << "only mysql is supported" << endl;
    exit(1);
  }
  tmd::conn_t dbh(*opt_mysql_host, *opt_mysql_user, *opt_mysql_password,
		  *opt_database, *opt_mysql_port);
  
  // handle the command
  if (command == "create-trigger") {
    vector<string> stmt(mgr->create_trigger_all(false));
    run_all_stmt(dbh, stmt);
  } else if (command == "drop-trigger") {
    vector<string> stmt(mgr->drop_trigger_all(true));
    run_all_stmt(dbh, stmt);
  } else if (command == "print-trigger") {
    vector<string> stmt(mgr->create_trigger_all(false));
    picojson::value a(picojson::array_type, false);
    copy(stmt.begin(), stmt.end(), back_inserter(a.get<picojson::array>()));
    a.serialize(ostream_iterator<char>(cout));
    cout << endl;
  } else if (command == "create-queue") {
    vector<string> stmt(aq_driver()->create_table_all(false, dbh));
    run_all_stmt(dbh, stmt);
  } else if (command == "drop-queue") {
    vector<string> stmt(aq_driver()->drop_table_all(true));
    run_all_stmt(dbh, stmt);
  } else if (command == "forward") {
    pthread_t* thr = new pthread_t [mgr->defs().size()];
    for (size_t i = 0; i < mgr->defs().size(); ++i) {
      const incline_def_async_qtable* def
	= static_cast<incline_def_async_qtable*>(mgr->defs()[i]);
      tmd::conn_t* dbh
	= new tmd::conn_t(*opt_mysql_host, *opt_mysql_user, *opt_mysql_password,
			  *opt_database, *opt_mysql_port);
      incline_driver_async_qtable::forwarder* fw
	= new incline_driver_async_qtable::forwarder(aq_driver(), def, dbh, 1);
      pthread_create(thr + i, NULL, incline_driver_async_qtable::forwarder::run,
		     fw);
    }
    for (size_t i = 0; i < mgr->defs().size(); ++i) {
      pthread_join(thr[i], NULL);
    }
    delete thr;
  } else {
    fprintf(stderr, "unknown command: %s\n", command.c_str());
    exit(1);
  }
  
  return 0;
}
