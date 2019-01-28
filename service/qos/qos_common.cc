/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include "qos_common.hh"
namespace qos {

bool operator==(const service_level_options &lhs, const service_level_options &rhs) {
    return lhs.shares == rhs.shares;
}
bool operator!=(const service_level_options &lhs, const service_level_options &rhs) {
    return !(lhs == rhs);
}
}
