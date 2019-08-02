/*
 * Copyright (C) 2019 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */


#pragma once

#include <ldap.h>
#include <memory>

#include <seastar/core/iostream.hh>
#include <seastar/core/posix.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/sstring.hh>
#include <seastar/net/api.hh>

/// A connection to an LDAP server with custom networking over a Seastar socket.  Constructor takes
/// a connected socket and generates an LDAP structure hooked up to it.  The LDAP object is obtained
/// using get_ldap(); its custom networking is valid as long as its ldap_connection host is alive.
///
/// WARNING: Seastar networking runs in separate fibers, which can be starved by blocking operations
/// in the fiber using LDAP.  You should not invoke synchronous LDAP operations or blocking
/// ldap_result().
class ldap_connection {
    seastar::file_desc _fd; ///< Libldap polls this to determine if we're ready for reading/writing.
    seastar::connected_socket _socket;
    seastar::input_stream<char> _input_stream; ///< _socket's input.
    seastar::output_stream<char> _output_stream; ///< _socket's output.
    seastar::temporary_buffer<char> _read_buffer; ///< Everything read by Seastar but not yet consumed by Sockbuf_IO.
    enum class status {
        // Lowercase, to avoid inadvertently invoking macros:
        up,             ///< Connected, operating normally.
        down,           ///< Shut down.
        eof,            ///< Read encountered EOF, write should be OK.
        err             ///< IO error encountered.
    };
    status _status; ///< When not OK, all \c read() and \c write() calls will immediately return without action.
    seastar::future<> _read_consumer; ///< Consumes Seastar read data.
    bool _read_in_progress; ///< Is there a Seastar read in progress?
    seastar::future<> _outstanding_write; ///< Captures Seastar write continuation.

    /// Deallocates an LDAP structure.
    struct ldap_deleter {
        void operator()(LDAP*);
    };
    std::unique_ptr<LDAP, ldap_deleter> _ldap;

  public:
    /// Creates LDAP with custom Seastar networking.
    ldap_connection(seastar::connected_socket&& socket);

    /// A pointer to the LDAP customized with Seastar IO.  *this keeps ownership.
    LDAP* get_ldap() const { return _ldap.get(); }

    /// Before destroying *this, user must wait on the future returned:
    seastar::future<> close();

    /// The last error reported by an LDAP operation.
    seastar::sstring get_error() const;

    /// Cannot be moved, since it spawns continuations that capture \c this.
    ldap_connection(ldap_connection&&) = delete;

  private:
    // Sockbuf_IO functionality (see Sockbuf_IO manpage):
    static int sbi_ctrl(Sockbuf_IO_Desc* sid, int option, void* value) noexcept;
    static ber_slen_t sbi_read(Sockbuf_IO_Desc* sid, void* buffer, ber_len_t size) noexcept;
    static ber_slen_t sbi_write(Sockbuf_IO_Desc* sid, void* buffer, ber_len_t size) noexcept;
    static int sbi_close(Sockbuf_IO_Desc* sid) noexcept;
    static const Sockbuf_IO seastar_sbio;

    /// Schedules a Seastar write of (copied) b[:size].  On success, returns size.  On failure,
    /// returns LDAP's failure code.
    ber_slen_t write(char const* b, ber_len_t size);

    /// Consumes at most \p size bytes from what Seastar has read so far and writes them to b.  On
    /// success, returns the number of bytes so consumed (possibly less than \p size).  On failure,
    /// returns LDAP's failure code.
    ber_slen_t read(char* b, ber_len_t size);

    /// Shuts down all internal state that can be shut down immediately.  See also close().
    void shutdown();

    /// Initiates a Seastar read that will procure data for \c read() to consume.  Data consumption
    /// will happen in a future captured by _read_consumer.  If _read_consumer is currently active,
    /// however (because the previous Seastar read hasn't been consumed yet), this method does
    /// nothing.
    void read_ahead();
};

/// Creates an ldap_connection, invokes a function on it, then waits for its closing.  Must be
/// invoked from Seastar thread.
void with_ldap_connection(seastar::connected_socket&&, std::function<void(ldap_connection&)>);

/// Connects to an address, then invokes with_ldap_connection on the resulting socket.  Must be
/// invoked from Seastar thread.
void with_ldap_connection(const seastar::socket_address&, std::function<void(ldap_connection&)>);
