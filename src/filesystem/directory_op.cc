#include <algorithm>
#include <sstream>

#include "filesystem/directory_op.h"

namespace chfs {

/**
 * Some helper functions
 */
auto string_to_inode_id(std::string &data) -> inode_id_t {
    std::stringstream ss(data);
    inode_id_t inode;
    ss >> inode;
    return inode;
}

auto string_to_inode_id(const std::string &data) -> inode_id_t {
    std::stringstream ss(data);
    inode_id_t inode;
    ss >> inode;
    return inode;
}

auto inode_id_to_string(inode_id_t id) -> std::string {
    std::stringstream ss;
    ss << id;
    return ss.str();
}

// {Your code here}
auto dir_list_to_string(const std::list<DirectoryEntry> &entries)
        -> std::string {
    std::ostringstream oss;
    for (const auto &entry: entries) {
        oss << entry.name << ':' << entry.id << '/';
    }
//    std::cerr << oss.str() << std::endl;
    return oss.str();
}

// {Your code here}
auto append_to_directory(std::string src, std::string filename, inode_id_t id)
        -> std::string {
    return src += filename + ":" + inode_id_to_string(id) + "/";
}

// {Your code here}
void parse_directory(std::string &src, std::list<DirectoryEntry> &list) {
    std::string entry;

    size_t pos = 0;
    size_t last_pos = 0;

    while (pos != std::string::npos && pos + 1 <= src.size()) {
        last_pos = src.find('/', pos + 1);
        entry = src.substr(pos, last_pos - pos);
        auto pos_mid = entry.rfind(':');

        auto name = entry.substr(0, pos_mid);

        std::stringstream ss(entry.substr(pos_mid + 1));
        inode_id_t inode;
        ss >> inode;
        list.push_back({name, inode});
        pos = last_pos + 1;
    }
}

// {Your code here}
auto rm_from_directory(std::string src, std::string filename) -> std::string {
    auto pos = src.find(filename);

    if (pos == std::string::npos) {
        return src;
    }

    auto pos_mid = src.find(':', pos);

    if (pos_mid == std::string::npos) {
        return src;
    }

    if (src.substr(pos, pos_mid - pos) == filename) {
        auto pos_end = src.find('/', pos_mid);
        if (pos_end == std::string::npos) {
            return src;
        }

        if (pos_end + 1 == src.size()) {
            return src.substr(0, pos);
        } else {
            return src.substr(0, pos) + src.substr(pos_end + 1);
        }
    }
    return src;
}

/**
 * { Your implementation here }
 */
auto read_directory(FileOperation *fs, inode_id_t id,
                    std::list<DirectoryEntry> &list) -> ChfsNullResult {
    auto res = fs->read_file(id);

    auto data = res.unwrap();

    std::string data_str(reinterpret_cast<const char *>(res.unwrap().data()),
                         data.size());

    parse_directory(data_str, list);

    return KNullOk;
}

auto read_directory(const std::shared_ptr<FileOperation> &fs, inode_id_t id,
                    std::list<DirectoryEntry> &list) -> ChfsNullResult {
    auto res = fs->read_file(id);

    auto data = res.unwrap();

    std::string data_str(reinterpret_cast<const char *>(res.unwrap().data()),
                         data.size());

    parse_directory(data_str, list);

    return KNullOk;
}

// {Your code here}
auto FileOperation::lookup(inode_id_t id, const char *name)
        -> ChfsResult<inode_id_t> {
    std::list<DirectoryEntry> list;

    auto res = read_directory(this, id, list);

    if (res.is_err()) {
        return {res.unwrap_error()};
    }

    for (const auto &entry: list) {
        if (strcmp(entry.name.c_str(), name) == 0) {
            return ChfsResult<inode_id_t>(entry.id);
        }
    }

    return ChfsResult<inode_id_t>(ErrorType::NotExist);
}

auto FileOperation::mk_helper(inode_id_t parent, const char *name, InodeType type, std::vector<std::shared_ptr<BlockOperation>> &ops, bool &is_error)
        -> ChfsResult<inode_id_t> {
    auto res = read_file(parent);

    if (res.is_err()) {
        return {res.unwrap_error()};
    }

    auto file_data = std::string{reinterpret_cast<char *>(res.unwrap().data()),
                                 res.unwrap().size()};

    {
        std::string name_str(name);
        auto pos = file_data.find(name);
        auto pos_end = file_data.find('/', pos);
        auto pos_mid = file_data.rfind(':', pos_end);

        // maybe file name contains ':'
        if (pos != std::string::npos && pos_end != std::string::npos &&
            pos_mid != std::string::npos && pos_mid == pos + name_str.size()) {
            return {ErrorType::AlreadyExist};
        }
    }

    auto new_inode = this->alloc_inode(type, ops, is_error);

    if (new_inode.is_err()) {
        return {new_inode.unwrap_error()};
    }


    auto new_entry = append_to_directory(file_data, name, new_inode.unwrap());
    std::vector<u8> old_data;
    old_data.resize(new_entry.size());
    old_data.assign(new_entry.begin(), new_entry.end());

    auto w_res = write_file(parent, old_data, ops, is_error);

    if (w_res.is_err()) {
        return {w_res.unwrap_error()};
    }

    return ChfsResult<inode_id_t>(new_inode.unwrap());
}

// {Your code here}
auto FileOperation::mk_helper(inode_id_t id, const char *name, InodeType type)
        -> ChfsResult<inode_id_t> {
    auto res = read_file(id);

    if (res.is_err()) {
        return {res.unwrap_error()};
    }

    auto file_data = std::string{reinterpret_cast<char *>(res.unwrap().data()),
                                 res.unwrap().size()};

    {
        std::string name_str(name);
        auto pos = file_data.find(name);
        auto pos_end = file_data.find('/', pos);
        auto pos_mid = file_data.rfind(':', pos_end);

        // maybe file name contains ':'
        if (pos != std::string::npos && pos_end != std::string::npos &&
            pos_mid != std::string::npos && pos_mid == pos + name_str.size()) {
            return {ErrorType::AlreadyExist};
        }
    }

    auto new_inode = this->alloc_inode(type);

    if (new_inode.is_err()) {
        return {new_inode.unwrap_error()};
    }

    auto new_entry = append_to_directory(file_data, name, new_inode.unwrap());
    std::vector<u8> old_data;
    old_data.resize(new_entry.size());
    old_data.assign(new_entry.begin(), new_entry.end());

    auto w_res = write_file(id, old_data);

    if (w_res.is_err()) {
        return {w_res.unwrap_error()};
    }

    return ChfsResult<inode_id_t>(new_inode.unwrap());
}
auto FileOperation::unlink_regular_file(inode_id_t parent, const char *name, std::vector<u8> &buf) -> ChfsNullResult {
    auto res = read_file(parent);

    if (res.is_err()) {
        return {res.unwrap_error()};
    }

    auto file_data = std::string{reinterpret_cast<char *>(res.unwrap().data()),
                                 res.unwrap().size()};

    std::string name_str(name);

    // maybe file name contains ':'
    auto pos = file_data.find(name_str);
    auto pos_end = file_data.find('/', pos);
    auto pos_mid = file_data.rfind(':', pos_end);

    if (pos == std::string::npos || (pos_mid != pos + name_str.size())) {
        return {ErrorType::NotExist};
    }

    auto inode_id =
            string_to_inode_id(file_data.substr(pos_mid + 1, pos_end - pos_mid - 1));


    auto tp_res = gettype(inode_id);

    if (tp_res.is_err()) {
        return {tp_res.unwrap_error()};
    }

    if (tp_res.unwrap() == InodeType::Directory) {
        auto rm_res = remove_file(inode_id);

        if (rm_res.is_err()) {
            return {rm_res.unwrap_error()};
        }

        std::list<DirectoryEntry> list;

        auto read_res = read_directory(this, parent, list);

        if (read_res.is_err()) {
            return {read_res.unwrap_error()};
        }

        auto src = rm_from_directory(dir_list_to_string(list), name);

        std::vector<u8> old_data;
        old_data.resize(src.size());
        old_data.assign(src.begin(), src.end());

        auto w_res = write_file(parent, old_data);

        if (w_res.is_err()) {
            return {w_res.unwrap_error()};
        }
        return KNullOk;
    }

    read_as_regular_file(inode_id, buf);

    auto rm_res = remove_file(inode_id);

    if (rm_res.is_err()) {
        return {rm_res.unwrap_error()};
    }


    std::list<DirectoryEntry> list;

    auto read_res = read_directory(this, parent, list);

    if (read_res.is_err()) {
        return {read_res.unwrap_error()};
    }

    auto src = rm_from_directory(dir_list_to_string(list), name);

    std::vector<u8> old_data;
    old_data.resize(src.size());
    old_data.assign(src.begin(), src.end());

    auto w_res = write_file(parent, old_data);

    if (w_res.is_err()) {
        return {w_res.unwrap_error()};
    }


    return KNullOk;
}

auto FileOperation::unlink_regular_file(inode_id_t parent, const char *name, std::vector<u8> &buf, std::vector<std::shared_ptr<BlockOperation>> &ops, bool &is_err) -> ChfsNullResult {
    auto res = read_file(parent);

    if (res.is_err()) {
        return {res.unwrap_error()};
    }

    auto file_data = std::string{reinterpret_cast<char *>(res.unwrap().data()),
                                 res.unwrap().size()};

    std::string name_str(name);

    // maybe file name contains ':'
    auto pos = file_data.find(name_str);
    auto pos_end = file_data.find('/', pos);
    auto pos_mid = file_data.rfind(':', pos_end);

    if (pos == std::string::npos || (pos_mid != pos + name_str.size())) {
        return {ErrorType::NotExist};
    }

    auto inode_id =
            string_to_inode_id(file_data.substr(pos_mid + 1, pos_end - pos_mid - 1));


    auto tp_res = gettype(inode_id);

    if (tp_res.is_err()) {
        return {tp_res.unwrap_error()};
    }

    if (tp_res.unwrap() == InodeType::Directory) {
        auto rm_res = remove_file(inode_id);

        if (rm_res.is_err()) {
            return {rm_res.unwrap_error()};
        }

        std::list<DirectoryEntry> list;

        auto read_res = read_directory(this, parent, list);

        if (read_res.is_err()) {
            return {read_res.unwrap_error()};
        }

        auto src = rm_from_directory(dir_list_to_string(list), name);

        std::vector<u8> old_data;
        old_data.resize(src.size());
        old_data.assign(src.begin(), src.end());

        auto w_res = write_file(parent, old_data, ops, is_err);

        if (w_res.is_err()) {
            return {w_res.unwrap_error()};
        }
        return KNullOk;
    }

    read_as_regular_file(inode_id, buf);

    auto rm_res = remove_file(inode_id);

    if (rm_res.is_err()) {
        return {rm_res.unwrap_error()};
    }


    std::list<DirectoryEntry> list;

    auto read_res = read_directory(this, parent, list);

    if (read_res.is_err()) {
        return {read_res.unwrap_error()};
    }

    auto src = rm_from_directory(dir_list_to_string(list), name);

    std::vector<u8> old_data;
    old_data.resize(src.size());
    old_data.assign(src.begin(), src.end());

    auto w_res = write_file(parent, old_data, ops, is_err);

    if (w_res.is_err()) {
        return {w_res.unwrap_error()};
    }


    return KNullOk;
}

// {Your code here}
auto FileOperation::unlink(inode_id_t parent, const char *name)
        -> ChfsNullResult {
    auto res = read_file(parent);

    if (res.is_err()) {
        return {res.unwrap_error()};
    }

    auto file_data = std::string{reinterpret_cast<char *>(res.unwrap().data()),
                                 res.unwrap().size()};

    std::string name_str(name);

    // maybe file name contains ':'
    auto pos = file_data.find(name_str);
    auto pos_end = file_data.find('/', pos);
    auto pos_mid = file_data.rfind(':', pos_end);

    if (pos == std::string::npos || (pos_mid != pos + name_str.size())) {
        return {ErrorType::NotExist};
    }

    auto inode_id =
            string_to_inode_id(file_data.substr(pos_mid + 1, pos_end - pos_mid - 1));


    auto tp_res = gettype(inode_id);

    if (tp_res.is_err()) {
        return {tp_res.unwrap_error()};
    }


    if (tp_res.unwrap() == InodeType::Directory) {
        return {ErrorType::NotEmpty};
    }

    auto rm_res = remove_file(inode_id);

    if (rm_res.is_err()) {
        return {rm_res.unwrap_error()};
    }

    std::list<DirectoryEntry> list;

    auto read_res = read_directory(this, parent, list);

    if (read_res.is_err()) {
        return {read_res.unwrap_error()};
    }

    auto src = rm_from_directory(dir_list_to_string(list), name);

    std::vector<u8> old_data;
    old_data.resize(src.size());
    old_data.assign(src.begin(), src.end());

    auto w_res = write_file(parent, old_data);

    if (w_res.is_err()) {
        return {w_res.unwrap_error()};
    }

    //  std::vector<u8> ddata(this->block_manager_->block_size());
    //
    //  this->inode_manager_->read_inode(parent, ddata);
    //
    //  auto inode_p = reinterpret_cast<Inode *>(ddata.data());
    //
    //  inode_p->inner_attr.set_all_time(time(0));
    //
    //  auto w_res2 = this->block_manager_->write_block(parent, ddata.data());
    //
    //  if (w_res2.is_err()) {
    //    return {w_res2.unwrap_error()};
    //  }

    return KNullOk;
}

}// namespace chfs
