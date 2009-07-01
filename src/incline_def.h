#ifndef incline_def_h
#define incline_def_h

#include <string>
#include <map>
#include <vector>

class incline_def {
protected:
  // destination table
  std::string destination_;
  // source tables
  std::vector<std::string> source_;
  // src_tbl.src_col => dest_primary_key_col
  std::map<std::string, std::string> pk_columns_;
  // src_tbl.src_col => dest_no_primary_key_col
  std::map<std::string, std::string> npk_columns_;
  // inner join conds, list of first=last
  std::vector<std::pair<std::string, std::string> > merge_;
  // pk_columns + npk_columns
  std::map<std::string, std::string> columns_;
  // reverse map of columns_
  std::map<std::string, std::string> dest_columns_;
public:
  virtual ~incline_def() {}
  // use default copy functions
  // accessors
  std::string destination() const { return destination_; }
  const std::vector<std::string>& source() const { return source_; }
  const std::map<std::string, std::string>& pk_columns() const { return pk_columns_; }
  const std::map<std::string, std::string>& npk_columns() const { return npk_columns_; }
  const std::vector<std::pair<std::string, std::string> >& merge() const { return merge_; }
  const std::map<std::string, std::string>& columns() const { return columns_; }
  const std::map<std::string, std::string>& dest_columns() const { return dest_columns_; }
  bool is_master_of(const std::string& table) const;
  bool is_dependent_of(const std::string& table) const;
  std::vector<std::string> build_merge_cond(const std::string& tbl_rewrite_from, const std::string& tbl_rewrite_to, bool master_only = false) const;
  // parser
  virtual std::string parse(const std::map<std::string>& def) const;
protected:
  virtual std::string do_parse_property(const std::string& name, const std::string& value) const;
  void _rebuild_columns();
public:
  static std::string table_of_column(const std::string& column);
};

#endif
