/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include "cql3/statements/sl_prop_defs.hh"
#include "database.hh"


namespace cql3 {

namespace statements {

void sl_prop_defs::validate() {
    static std::set<sstring> keywords({ sstring(KW_SHARES)});
    property_definitions::validate(keywords);
    _shares =  get_int(KW_SHARES, SHARES_DEFAULT_VAL);
    if ((_shares < SHARES_MIN_VAL) || (_shares > SHARES_MAX_VAL )) {
        throw exceptions::syntax_exception(format("'SHARES' can only take values of {}-{} (given {})",
                SHARES_MIN_VAL, SHARES_MAX_VAL, _shares));
    }
}

int sl_prop_defs::get_shares() {
    return _shares;
}

}

}
