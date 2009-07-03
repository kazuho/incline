#include <fstream>
#include <iostream>
#include <iterator>
#include "getoptpp.h"
#include "incline.h"

using namespace std;

static getoptpp::opt_str opt_mode('m', "mode", false, "mode", "standalone");
static getoptpp::opt_str opt_source('s', "source", true, "definition file");
static getoptpp::opt_str opt_rdbms('r', "rdbms", false, "rdbms name", "mysql");
static getoptpp::opt_str opt_database('d', "database", true, "database name");

static getoptpp::opt_str opt_mysql_host(0, "mysql-host", false, "mysql host",
					"localhost");
static getoptpp::opt_str opt_mysql_user(0, "mysql-user", false,
					"mysql user", "root");
static getoptpp::opt_str opt_mysql_password(0, "mysql-password", false,
					    "mysql password", "");
static getoptpp::opt_int opt_mysql_port(0, "mysql-port", false, "mysql port",
					3306);

static void run_all_stmt(tmd::conn_t dbh, const vector<string>& stmt)
{
  for (vector<string>::const_iterator si = stmt.begin();
       si != stmt.end();
       ++si) {
    tmd::execute(dbh, si->c_str());
  }
}

int
main(int argc, char** argv)
{
  string command;
  picojson::value defs;
  
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
  
  // create manager
  incline_driver* driver;
  if (*opt_mode == "standalone") {
    driver = new incline_driver_standalone();
  } else if (*opt_mode == "async_qtable") {
    driver = new incline_driver_async_qtable();
  } else {
    cerr << "unknown mode:" << *opt_mode << endl;
    exit(1);
  }
  incline_mgr mgr(driver);
  
  { // parse source
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
      err = mgr.parse(defs);
    }
    if (! err.empty()) {
      cerr << "failed to parse file:" << *opt_source << ": " << err << endl;
      exit(3);
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
    vector<string> stmt(mgr.create_trigger_all(true));
    run_all_stmt(dbh, stmt);
  } else if (command == "drop-trigger") {
    vector<string> stmt(mgr.drop_trigger_all(true));
    run_all_stmt(dbh, stmt);
  } else if (command == "print-trigger") {
    vector<string> stmt(mgr.create_trigger_all(true));
    picojson::value a(picojson::array_type, false);
    copy(stmt.begin(), stmt.end(), back_inserter(a.get<picojson::array>()));
    a.serialize(ostream_iterator<char>(cout));
    cout << endl;
  } else {
    fprintf(stderr, "unknown command: %s\n", command.c_str());
    exit(1);
  }
  
  return 0;
}
