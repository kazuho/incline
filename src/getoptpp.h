#ifndef getoptpp_h
#define getoptpp_h

extern "C" {
#include <getopt.h>
}
#include <cassert>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string>
#include <vector>

namespace getoptpp {
  
  class opt_base;
  typedef std::vector<opt_base*> opt_list_t;
  enum {
    opt_template_length = 30
  };
  
  template <typename T> struct _opt_list {
    static opt_list_t* opts;
    static void register_opt(opt_base* opt) {
      if (opts == NULL) {
	opts = new opt_list_t();
      }
      opts->push_back(opt);
    }
    static void unregister_opt(opt_base* opt) {
      assert(opts != NULL);
      opt_list_t::iterator i = std::find(opts->begin(), opts->end(), opt);
      assert(i != opts->end());
      opts->erase(i);
      if (opts->size() == 0) {
	delete opts;
	opts = NULL;
      }
    }
  };
  template <typename T> opt_list_t* _opt_list<T>::opts;
  typedef _opt_list<bool> opt_list;
  
  class opt_base {
  protected:
    option o_;
    char shortname_;
    bool die_on_validate_;
    std::string desc_;
  public:
    opt_base(char shortname, const char* longname, int has_arg, bool required,
	     const std::string& desc)
      : shortname_(shortname), die_on_validate_(required), desc_(desc) {
      o_.name = longname;
      o_.has_arg = has_arg;
      o_.flag = NULL;
      o_.val = 0;
      opt_list::register_opt(this);
    }
    virtual ~opt_base() {
      opt_list::unregister_opt(this);
    }
    char shortname() const { return shortname_; }
    const option& get_option() const { return o_; }
    virtual void handle() {
      die_on_validate_ = false;
    }
    virtual bool validate() {
      if (die_on_validate_) {
	std::cerr << "option --" << o_.name << " not set" << std::endl;
	return false;
      }
      return true;
    }
    void help(std::ostream& os) {
      std::string s("  --");
      s += o_.name;
      switch (o_.has_arg) {
      case required_argument: s += '='; break;
      default: break;
      }
      if (s.size() < opt_template_length) {
	s.insert(s.end(), opt_template_length - s.size(), ' ');
	os << s << "  " << desc_;
      } else {
	os << s << std::endl << std::string(opt_template_length + 2, ' ')
	   << desc_;
      }
    }
  };
  
  class opt_flag : public opt_base {
  protected:
    bool flag_;
  public:
    opt_flag(char shortname, const char* longname, const std::string& desc)
      : opt_base(shortname, longname, no_argument, false, desc), flag_(false)
      {}
    virtual void handle() {
      flag_ = true;
    }
    const bool operator*() const { return flag_; }
  };
  
  template <typename T> class opt_value_base : public opt_base {
  protected:
    T value_;
  public:
    opt_value_base(char shortname, const char* longname, bool required,
		   const std::string& desc, const T& defval)
      : opt_base(shortname, longname, required_argument, required, desc),
	value_(defval) {}
    const T& operator*() const { return value_; }
    T& operator*() { return value_; }
    const T* operator->() const { return &value_; }
    T* operator->() { return &value_; }
  };
  
  class opt_str : public opt_value_base<std::string> {
  public:
    opt_str(char shortname, const char* longname, bool required,
	    const std::string& desc, const std::string& defval = std::string())
      : opt_value_base<std::string>(shortname, longname, required, desc,
				    defval) {}
    virtual void handle() {
      opt_value_base<std::string>::handle();
      value_ = optarg;
    }
  };
  
  class opt_int : public opt_value_base<int> {
  public:
    opt_int(char shortname, const char* longname, bool required,
	    const std::string& desc, const int& defval = 0)
      : opt_value_base<int>(shortname, longname, required, desc, defval) {}
    virtual void handle() {
      opt_value_base<int>::handle();
      if (sscanf(optarg, "%d", &value_) != 1) {
	std::cerr << "--" << get_option().name << " only accepts numbers"
		  << std::endl;
	exit(1);
      }
    }
  };
  
  class opt_version : public opt_flag {
  protected:
    std::string version_;
  public:
    opt_version(char shortname, const char* longname,
		const std::string& version)
      : opt_flag(shortname, longname, "print version"), version_(version) {}
    virtual void handle() {
      std::cout << version_ << std::endl;
      exit(0);
    }
  };
  
  class opt_help : public opt_base {
  protected:
    std::string progname_;
    std::string commands_;
  public:
    opt_help(char shortname, const char* longname, const std::string& progname,
	     const std::string& commands)
      : opt_base(shortname, longname, no_argument, false, "print this help"),
	progname_(progname), commands_(commands) {}
    virtual void handle() {
      std::cout << "Usage: " << progname_ << " options " << commands_
		<< std::endl;
      std::cout << "Options: " << std::endl;
      for (opt_list_t::const_iterator i = opt_list::opts->begin();
	   i != opt_list::opts->end();
	   ++i) {
	std::cout << "  ";
	(*i)->help(std::cout);
	std::cout << std::endl;
      }
      std::cout << std::endl;
      exit(0);
    }
  };
  
  inline bool getopt(int argc, char** argv) {
    std::string optstr;
    option* opts = new option [opt_list::opts->size() + 1];
    for (size_t i = 0; i < opt_list::opts->size(); ++i) {
      opts[i] = (*opt_list::opts)[i]->get_option();
      if (char ch = (*opt_list::opts)[i]->shortname()) {
	optstr.push_back(ch);
      }
      if (opts[i].has_arg != no_argument) {
	optstr.push_back(':');
      }
    }
    memset(opts + opt_list::opts->size(), 0, sizeof(option));
    int ch, longidx;
    while ((ch = getopt_long(argc, argv, optstr.c_str(), opts, &longidx))
	   != -1) {
      if (ch == ':' || ch == '?') {
	return false;
      }
      (*opt_list::opts)[longidx]->handle();
    }
    bool success = true;
    for (opt_list_t::iterator i = opt_list::opts->begin();
	 i != opt_list::opts->end();
	 ++i) {
      if (! (*i)->validate()) {
	success = false;
      }
    }
    return success;
  }
}

#endif
