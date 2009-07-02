extern "C" {
#include <getopt.h>
}
#include "incline.h"

using namespace std;

static void usage(int argc, char** argv)
{
  printf("Usage: %s --driver=d command\n", argv[0]);
  exit(0);
}

int
main(int argc, char** argv)
{
  string driver_name, command;
  
  { // parse commond
    static struct option longopts[] = {
      { "driver", required_argument, NULL, 'd' },
      { "help",   no_argument,       NULL, 'h' },
      { NULL,     0,                 NULL, 0 },
    };
    int ch;
    while ((ch = getopt_long(argc, argv, "d:h", longopts, NULL)) != -1) {
      switch (ch) {
      case 'd':
	driver_name = optarg;
	break;
      case 'h':
	usage(argc, argv);
	break;
      default:
	exit(1);
	break;
      }
    }
    argc -= optind;
    argv += optind;
    if (argc == 0) {
      fprintf(stderr, "no command\n");
      exit(1);
    }
    command = *argv++;
    argc--;
    if (driver_name.empty()) {
      fprintf(stderr, "--driver not set\n");
      exit(1);
    }
  }
  
  { // do it
    incline_driver* driver;
    if (driver_name == "standalone") {
      driver = new incline_driver_standalone();
    } else if (driver_name == "async_qtable") {
      driver = new incline_driver_async_qtable();
    } else {
      fprintf(stderr, "unknown driver: %s\n", driver_name.c_str());
      exit(1);
    }
    incline_mgr mgr(driver);
    if (command == "print-trigger") {
      vector<string> stmt(mgr.create_trigger_all(true));
      fputs(incline_util::join('\n', stmt.begin(), stmt.end()).c_str(),
	    stdout);
    } else {
      fprintf(stderr, "unknown command: %s\n", command.c_str());
      exit(1);
    }
  }
  
  return 0;
}