/*
 * Copyright (C) 2015 ScyllaDB
 *
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */
#include <map>
#include <unordered_map>
#include <tuple>
#include <stdexcept>
#include <regex>
#include <algorithm>

#include <openssl/evp.h>
#include <openssl/rand.h>
#include <openssl/md5.h>
#include <openssl/sha.h>

#include <boost/range/adaptor/map.hpp>
#include <boost/filesystem.hpp>

#include <seastar/core/seastar.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/reactor.hh>

#include "compress.hh"
#include "encryption.hh"
#include "symmetric_key.hh"
#include "local_file_provider.hh"
#include "replicated_key_provider.hh"
#include "kmip_key_provider.hh"
#include "kmip_host.hh"
#include "bytes.hh"
#include "utils/class_registrator.hh"
#include "db/extensions.hh"
#include "db/system_keyspace.hh"
#include "utils/class_registrator.hh"
#include "serializer.hh"
#include "serializer_impl.hh"
#include "schema.hh"
#include "sstables/sstables.hh"
#include "db/commitlog/commitlog_extensions.hh"
#include "encrypted_file_impl.hh"
#include "encryption_config.hh"

namespace encryption {

static const std::set<sstring> keywords = { KEY_PROVIDER,
                SECRET_KEY_PROVIDER_FACTORY_CLASS, SECRET_KEY_FILE, SYSTEM_KEY_FILE,
                CIPHER_ALGORITHM, IV_LENGTH, SECRET_KEY_STRENGTH, HOST_NAME,
                TEMPLATE_NAME, KEY_NAMESPACE
};

static constexpr auto REPLICATED_KEY_PROVIDER_FACTORY = "ReplicatedKeyProviderFactory";
static constexpr auto LOCAL_FILE_SYSTEM_KEY_PROVIDER_FACTORY = "LocalFileSystemKeyProviderFactory";
static constexpr auto KMIP_KEY_PROVIDER_FACTORY = "KmipKeyProviderFactory";

bytes base64_decode(const sstring& s, size_t off, size_t len) {
    if (off >= s.size()) {
        throw std::out_of_range("Invalid offset");
    }
    len = std::min(len, s.size() - off);
    auto n = (len / 4) * 3;
    bytes b{bytes::initialized_later(), n};

    // EVP_DecodeBlock does not handle padding well (i.e. it returns
    // data with actual padding. This is not what we want, since
    // we need to allow zeros in data.
    // Must thus do decoding the hard way...

    std::unique_ptr<EVP_ENCODE_CTX, void (*)(EVP_ENCODE_CTX*)> ctxt(EVP_ENCODE_CTX_new(), &EVP_ENCODE_CTX_free);

    ::EVP_DecodeInit(ctxt.get());

    int outl = 0;
    auto r = ::EVP_DecodeUpdate(ctxt.get(), reinterpret_cast<uint8_t*>(b.data()), &outl, reinterpret_cast<const uint8_t *>(s.data() + off),
                    int(len));
    if (r < 0) {
        throw std::invalid_argument("Could not decode: " + s);
    }

    int outl2 = 0;
    r = ::EVP_DecodeFinal(ctxt.get(), reinterpret_cast<uint8_t*>(b.data() + outl), &outl2);
    if (r < 0) {
        throw std::invalid_argument("Could not decode: " + s);
    }
    b.resize(outl + outl2);
    return b;
}

sstring base64_encode(const bytes& b, size_t off, size_t len) {
    if (off >= b.size()) {
        throw std::out_of_range("Invalid offset");
    }
    len = std::min(len, b.size() - off);
    auto n = ((len + 2) / 3) * 4;
    sstring s{sstring::initialized_later(), n};
    auto r = EVP_EncodeBlock(reinterpret_cast<uint8_t *>(s.data()),
                    reinterpret_cast<const uint8_t*>(b.data() + off), int(len));
    if (r < 0) {
        throw std::invalid_argument("Could not encode");
    }
    s.resize(r);
    return s;
}

bytes calculate_md5(const bytes& b, size_t off, size_t len) {
    if (off >= b.size()) {
        throw std::out_of_range("Invalid offset");
    }
    len = std::min(len, b.size() - off);
    bytes res{bytes::initialized_later(), MD5_DIGEST_LENGTH};
    MD5(reinterpret_cast<const uint8_t*>(b.data() + off), len, reinterpret_cast<uint8_t *>(res.data()));
    return res;
}

bytes calculate_sha256(const bytes& b, size_t off, size_t len) {
    if (off >= b.size()) {
        throw std::out_of_range("Invalid offset");
    }
    len = std::min(len, b.size() - off);
    bytes res{bytes::initialized_later(), SHA256_DIGEST_LENGTH};
    SHA256(reinterpret_cast<const uint8_t*>(b.data() + off), len, reinterpret_cast<uint8_t *>(res.data()));
    return res;
}

future<temporary_buffer<char>> read_text_file_fully(const sstring& filename) {
    return open_file_dma(filename, open_flags::ro).then([](file f) {
        return f.size().then([f](size_t s) {
            return do_with(make_file_input_stream(f), [s](input_stream<char>& in) {
                return in.read_exactly(s).then([](temporary_buffer<char> buf) {
                    return make_ready_future<temporary_buffer<char>>(std::move(buf));
                }).finally([&in] {
                    return in.close();
                });
            });
        });
    });
}

future<> write_text_file_fully(const sstring& filename, temporary_buffer<char> buf) {
    return open_file_dma(filename, open_flags::wo|open_flags::create).then([buf = std::move(buf)](file f) mutable {
        return do_with(make_file_output_stream(f), [buf = std::move(buf)](output_stream<char>& out) mutable {
            auto p = buf.get();
            auto s = buf.size();
            return out.write(p, s).finally([&out, buf = std::move(buf)] {
                return out.close();
            });
        });
    });
}

future<> write_text_file_fully(const sstring& filename, const sstring& s) {
    return write_text_file_fully(filename, temporary_buffer<char>(s.data(), s.size()));
}

static const sstring namespace_prefix = "com.datastax.bdp.cassandra.crypto.";
static const sstring encryption_attribute = "scylla_encryption_options";

key_info get_key_info(const options& map) {
    opt_wrapper opts(map);

    auto cipher_name = opts(CIPHER_ALGORITHM).value_or("AES/CBC/PKCS5Padding");
    auto key_strength = std::stoul(opts(SECRET_KEY_STRENGTH).value_or("128"));
    //TODO: handle user iv length? Does not play well with most impls...
    if (opts("IV_LENGTH")) {
        throw std::invalid_argument("User defined IV length not supported");
    }
    // todo: static constexpr auto KMIP_KEY_PROVIDER_FACTORY = "KmipKeyProviderFactory";
    return key_info{ std::move(cipher_name), unsigned(key_strength) };
}

class encryption_context_impl : public encryption_context {
    // poor mans per-thread instance variable. We need a lookup map
    // per shard, so preallocate it, much like a "sharded" thing would,
    // but without all the fancy start/stop stuff.
    // Allows this object to be effectively stateless, except for the
    // objects in the maps.
    std::vector<std::unordered_map<sstring, shared_ptr<key_provider>>> _per_thread_provider_cache;
    std::vector<std::unordered_map<sstring, shared_ptr<system_key>>> _per_thread_system_key_cache;
    std::vector<std::unordered_map<sstring, shared_ptr<kmip_host>>> _per_thread_kmip_host_cache;
    const encryption_config& _cfg;
public:
    encryption_context_impl(const encryption_config& cfg)
        : _per_thread_provider_cache(smp::count)
        , _per_thread_system_key_cache(smp::count)
        , _per_thread_kmip_host_cache(smp::count)
        , _cfg(cfg)
    {}

    shared_ptr<key_provider> get_provider(const options& map) override {
        opt_wrapper opts(map);

        auto provider_class = opts(KEY_PROVIDER);
        if (!provider_class) {
            provider_class = opts(SECRET_KEY_PROVIDER_FACTORY_CLASS).value_or(REPLICATED_KEY_PROVIDER_FACTORY);
        }

        static const std::unordered_map<sstring, std::unique_ptr<key_provider_factory>> providers = [] {
            std::unordered_map<sstring, std::unique_ptr<key_provider_factory>> map;

            map[REPLICATED_KEY_PROVIDER_FACTORY] = std::make_unique<replicated_key_provider_factory>();
            map[LOCAL_FILE_SYSTEM_KEY_PROVIDER_FACTORY] = std::make_unique<local_file_provider_factory>();
            map[KMIP_KEY_PROVIDER_FACTORY] = std::make_unique<kmip_key_provider_factory>();

            return map;
        }();

        unqualified_name qn(namespace_prefix, *provider_class);

        try {
            return providers.at(qn)->get_provider(*this, map);
        } catch (std::out_of_range&) {
            throw std::invalid_argument("Unknown provider: " + *provider_class);
        }
    }
    shared_ptr<key_provider> get_cached_provider(const sstring& id) const override {
        auto& cache = _per_thread_provider_cache[engine().cpu_id()];
        auto i = cache.find(id);
        if (i != cache.end()) {
            return i->second;
        }
        return {};
    }
    void cache_provider(const sstring& id, shared_ptr<key_provider> p) override {
        _per_thread_provider_cache[engine().cpu_id()][id] = std::move(p);
    }

    shared_ptr<system_key> get_system_key(const sstring& name) override {
        auto& cache = _per_thread_system_key_cache[engine().cpu_id()];
        auto i = cache.find(name);
        if (i != cache.end()) {
            return i->second;
        }

        shared_ptr<encryption::system_key> k;

        if (kmip_system_key::is_kmip_path(name)) {
            k = make_shared<kmip_system_key>(*this, name);
        } else {
            k = make_shared<local_system_key>(*this, name);
        }

        if (k != nullptr) {
            cache[name] = k;
        }

        return k;
    }

    shared_ptr<kmip_host> get_kmip_host(const sstring& host) override {
        auto& cache = _per_thread_kmip_host_cache[engine().cpu_id()];
        auto i = cache.find(host);
        if (i != cache.end()) {
            return i->second;
        }

        auto j = _cfg.kmip_hosts().find(host);
        if (j != _cfg.kmip_hosts().end()) {
            auto result = ::make_shared<kmip_host>(*this, host, j->second);
            cache.emplace(host, result);
            return result;
        }

        throw std::invalid_argument("No such host: "+ host);
    }

    const encryption_config& config() const override {
        return _cfg;
    }
};

class encryption_schema_extension : public schema_extension {
    key_info _info;
    shared_ptr<key_provider> _provider;
    std::map<sstring, sstring> _options;

public:
    encryption_schema_extension(key_info, shared_ptr<key_provider>, std::map<sstring, sstring>);

    using extension_ptr = ::shared_ptr<encryption_schema_extension>;

    static extension_ptr create(encryption_context&, std::map<sstring, sstring>);
    static extension_ptr parse(encryption_context& ctxt, db::extensions::schema_ext_config cfg) {
        struct {
            encryption_context& _ctxt;

            extension_ptr operator()(const sstring&) const {
                throw std::invalid_argument("Malformed extension");
            }
            extension_ptr operator()(const std::map<sstring, sstring>& opts) const {
                return create(_ctxt, opts);
            }
            extension_ptr operator()(const bytes& v) const {
                auto opts = ser::deserialize_from_buffer(v, boost::type<options>(), 0);
                return create(_ctxt, std::move(opts));
            }
        } v{ctxt};
        return std::visit(v, cfg);
    }

    future<::shared_ptr<symmetric_key>> key_for_read(opt_bytes id) const {
        return _provider->key(_info, std::move(id)).then([](auto k, auto id) {
            return std::move(k);
        });
    }
    future<::shared_ptr<symmetric_key>, opt_bytes> key_for_write() const {
        return _provider->key(_info);
    }

    bytes serialize() const override {
        return ser::serialize_to_buffer<bytes>(_options, 0);
    }
    future<> validate(const schema&) const override {
        return _provider->validate();
    }
};

encryption_schema_extension::encryption_schema_extension(key_info info, shared_ptr<key_provider> provider, std::map<sstring, sstring> options)
    : _info(std::move(info))
    , _provider(std::move(provider))
    , _options(std::move(options))
{}

::shared_ptr<encryption_schema_extension> encryption_schema_extension::create(encryption_context& ctxt, std::map<sstring, sstring> map) {
    key_info info = get_key_info(map);
    auto provider = ctxt.get_provider(map);
    return ::make_shared<encryption_schema_extension>(std::move(info), std::move(provider), std::move(map));
}

class encryption_file_io_extension : public sstables::file_io_extension {
    ::shared_ptr<encryption_context> _ctxt;
public:
    encryption_file_io_extension(::shared_ptr<encryption_context> ctxt)
        : _ctxt(std::move(ctxt))
    {}

    future<file> wrap_file(sstables::sstable& sst, sstables::component_type type, file f, open_flags flags) override {
        switch (type) {
        case sstables::component_type::Data:
            break;
        default:
            return make_ready_future<file>();
        }

        static const sstring key_id_attribute = "scylla_key_id";

        static const sstables::disk_string<uint32_t> encryption_attribute_ds{
            bytes{encryption_attribute.begin(), encryption_attribute.end()}
        };
        static const sstables::disk_string<uint32_t> key_id_attribute_ds{
            bytes{key_id_attribute.begin(), key_id_attribute.end()}
        };

        if (flags == open_flags::ro) {
            // open existing. check read opts.
            auto& sc = sst.get_shared_components();
            if (sc.scylla_metadata) {
                auto* exta  = sc.scylla_metadata->get_extension_attributes();
                if (exta) {
                    auto i = exta->map.find(encryption_attribute_ds);
                    if (i != exta->map.end()) {
                        auto esx = encryption_schema_extension::parse(*_ctxt, i->second.value);
                        opt_bytes id;

                        if (exta->map.count(key_id_attribute_ds)) {
                            id = exta->map.at(key_id_attribute_ds).value;
                        }

                        return esx->key_for_read(std::move(id)).then([esx, f](::shared_ptr<symmetric_key> k) {
                            return make_ready_future<file>(make_encrypted_file(f, std::move(k)));
                        });
                    }
                }
            }
        } else {
            auto s = sst.get_schema();
            auto e = s->extensions().find(encryption_attribute);
            if (e != s->extensions().end()) {
                auto esx = static_pointer_cast<encryption_schema_extension>(e->second);
                return esx->key_for_write().then([esx, f, &sst](::shared_ptr<symmetric_key> k, opt_bytes id) {
                    auto& sc = sst.get_shared_components();
                    if (!sc.scylla_metadata) {
                        sc.scylla_metadata.emplace();
                    }

                    auto& ext = sc.scylla_metadata->get_or_create_extension_attributes();
                    ext.map.emplace(encryption_attribute_ds, sstables::disk_string<uint32_t>{esx->serialize()});
                    if (id) {
                        ext.map.emplace(key_id_attribute_ds, sstables::disk_string<uint32_t>{*id});
                    }
                    return make_ready_future<file>(make_encrypted_file(f, std::move(k)));
                });
            }
        }
        return make_ready_future<file>();
    }
};

namespace bfs = boost::filesystem;

class encryption_commitlog_file_extension : public db::commitlog_file_extension {
    const ::shared_ptr<encryption_context> _ctxt;
    const options _opts;

    static const inline std::regex prop_expr = std::regex("^([^=]+)=(\\S+)$");
    static const inline sstring id_key = "key_id";

public:
    encryption_commitlog_file_extension(::shared_ptr<encryption_context> ctxt, options opts)
        : _ctxt(ctxt)
        , _opts(std::move(opts))
    {}
    sstring config_name(const sstring& filename) const {
        bfs::path p(filename);
        auto dir = p.parent_path();
        auto file = p.filename();
        return (dir / ("." + file.string())).string();
    }
    future<file> wrap_file(const sstring& filename, file f, open_flags flags) override {
        auto cfg_file = config_name(filename);

        if (flags == open_flags::ro) {
            return read_text_file_fully(cfg_file).then([f, this](temporary_buffer<char> buf) {
                std::istringstream ss(std::string(buf.begin(), buf.end()));
                options opts;
                std::string line;
                while (std::getline(ss, line)) {
                    std::smatch m;
                    if (std::regex_match(line, m, prop_expr)) {
                        auto k = m[1].str();
                        auto v = m[2].str();
                        opts[k] = v;
                    }
                }
                opt_bytes id;
                if (opts.count(id_key)) {
                    id = base64_decode(opts[id_key]);
                }
                return _ctxt->get_provider(opts)->key(get_key_info(opts), id).then([f](shared_ptr<symmetric_key> k, opt_bytes) {
                    return make_ready_future<file>(make_encrypted_file(f, k));
                });
            });
        } else {
            return _ctxt->get_provider(_opts)->key(get_key_info(_opts)).then([f, this, cfg_file](shared_ptr<symmetric_key> k, opt_bytes id) {
                std::ostringstream ss;
                for (auto&p : _opts) {
                    ss << p.first << "=" << p.second << std::endl;
                }
                if (id) {
                    ss << id_key << "=" << base64_encode(*id) << std::endl;
                }
                return write_text_file_fully(cfg_file, ss.str()).then([f, this, k] {
                    return make_ready_future<file>(make_encrypted_file(f, k));
                });
            });
        }
    }
    future<> before_delete(const sstring& filename) override {
        auto cfg_file = config_name(filename);
        return file_exists(cfg_file).then([cfg_file](bool b) {
            return b ? remove_file(cfg_file) : make_ready_future();
        });
    }
};

future<> register_extensions(const db::config&, const encryption_config& cfg, db::extensions& exts) {
    auto ctxt = ::make_shared<encryption_context_impl>(cfg);
    // Note: extensions are immutable and shared across shards.
    // Object in them must be stateless. We anchor the context in the
    // extension objects, and while it is not as such 100% stateless,
    // it is close enough.
    exts.add_schema_extension(encryption_attribute, [ctxt](auto v) {
        return encryption_schema_extension::parse(*ctxt, std::move(v));
    });
    exts.add_sstable_file_io_extension(encryption_attribute, std::make_unique<encryption_file_io_extension>(ctxt));
    options opts(cfg.system_info_encryption().begin(), cfg.system_info_encryption().end());
    opt_wrapper sie(opts);
    future<> f = make_ready_future<>();
    if (sie("enabled").value_or("false") == "true") {
        // commitlog/system table encryption should not use replicated keys,
        // We default to local keys, but KMIP should be ok as well.
        // TODO: maybe forbid replicated.
        opts[KEY_PROVIDER] = sie(KEY_PROVIDER).value_or(LOCAL_FILE_SYSTEM_KEY_PROVIDER_FACTORY);
        if (opts[KEY_PROVIDER] == LOCAL_FILE_SYSTEM_KEY_PROVIDER_FACTORY && !sie(SECRET_KEY_FILE)) {
            // system encryption uses different key folder than user tables.
            // explicitly set the key file path
            opts[SECRET_KEY_FILE] = (bfs::path(cfg.system_key_directory()) / bfs::path("system") / bfs::path(sie("key_name").value_or("system_table_keytab"))).string();
        }

        exts.add_commitlog_file_extension(encryption_attribute, std::make_unique<encryption_commitlog_file_extension>(ctxt, opts));

        // modify schemas for tables holding sensitive data to use encryption w. key described
        // by the opts.
        // since schemas are duplicated across shards, we must call to each shard and augument
        // them all.
        // Since we are in pre-init phase, this should be safe.
        f = f.then([opts, &exts] {
            return smp::invoke_on_all([opts, &exts] {
                auto& f = exts.schema_extensions().at(encryption_attribute);
                for (auto& s : { db::system_keyspace::paxos(), db::system_keyspace::batchlog() }) {
                    exts.add_extension_to_schema(s, encryption_attribute, f(opts));
                }
            });
        });
    }

    if (!cfg.kmip_hosts().empty()) {
        // only pre-create on shard 0.
        f = f.then([&] {
            return parallel_for_each(cfg.kmip_hosts(), [ctxt](auto& p) {
                auto host = ctxt->get_kmip_host(p.first);
                return host->connect();
            });
        });
    }

    return f;
}

}

