/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#define BOOST_TEST_MODULE core

#include <boost/test/unit_test.hpp>

#include "utils/murmur_hash.hh"
#include "bytes.hh"
#include <seastar/core/print.hh>

static const bytes full_sequence("012345678901234567890123456789012345678901234567890123456789");

static const uint64_t seed = 0xcafebabe;

// Below are pre-calculated results of hashing consecutive prefixes of full_sequence using hash3_x64_128(),
// staring from an empty prefix.
std::array<uint64_t,2> prefix_hashes[] = {
    {13907055927958326333ULL, 10701141902926764871ULL},
    {16872847325129109440ULL, 5125572542408278394ULL},
    {11916219991241122015ULL, 747256650753853469ULL},
    {1492790099208671403ULL,  16635411534431524239ULL},
    {16764172998150925140ULL, 7440789969466348974ULL},
    {6846275695158209935ULL,  11251493995290334439ULL},
    {1075204625779168927ULL,  3453614304122336174ULL},
    {1404180555660983881ULL,  13684781009779545989ULL},
    {10185829608361057848ULL, 1102754042417891721ULL},
    {12850382803381855486ULL, 7404649381971707328ULL},
    {972515366528881960ULL,   4507841639019527002ULL},
    {9279316204399455969ULL,  9712180353841837616ULL},
    {16558181491899334208ULL, 17507114537353308311ULL},
    {12977947643557220239ULL, 8334539845739718010ULL},
    {3743840537387886281ULL,  15297576726012815871ULL},
    {10675210326497176757ULL, 11200838847539594424ULL},
    {16363715880225337291ULL, 2866762944263215884ULL},
    {1272769995400892137ULL,  1744366104172354624ULL},
    {17426490373034063702ULL, 12666853004117709655ULL},
    {10757142341798556363ULL, 3984810732374497004ULL},
    {4593020710048021108ULL,  14359610319437287264ULL},
    {18212086870806388719ULL, 7490375939640747191ULL},
    {11209001888824275013ULL, 6491913312740217486ULL},
    {17601044365330203914ULL, 1779402119744049378ULL},
    {3916812090790925532ULL,  17533572508631620015ULL},
    {10113761195332211536ULL, 4163484992388084181ULL},
    {4353425943622404193ULL,  1830165015196477722ULL},
    {3904126367597302219ULL,  7917741892387588561ULL},
    {7077450301176172141ULL,  8070185570157969067ULL},
    {6331768922468785771ULL,  9311778359071820659ULL},
    {7715740891587706229ULL,  16510772505395753023ULL},
    {4510384582422222090ULL,  9352450339278885986ULL},
    {6746132289648898302ULL,  15402380546251654069ULL},
    {1315904697672087497ULL,  2686857386486814319ULL},
    {16122226135709041149ULL, 1278536837434550412ULL},
    {6449104926034509627ULL,  8809488279970194649ULL},
    {9047965986959166273ULL,  14963749820458851455ULL},
    {18095596803119563681ULL, 2806499127062067052ULL},
    {545238237267145238ULL,   4583663570136224396ULL},
    {12335897404061220746ULL, 8643308333771385742ULL},
    {15016951849151361171ULL, 13012972687708005422ULL},
    {12896848725136832414ULL, 9881710852371170521ULL},
    {17900663530283054991ULL, 9606960248070178723ULL},
    {4513619521783122834ULL,  4823611535250518791ULL},
    {15572858348470724038ULL, 4882998878774456634ULL},
    {3464540909110937960ULL,  14591983318346304410ULL},
    {2951301498066556278ULL,  3029976006973164807ULL},
    {7848995488883197496ULL,  10621954303326018594ULL},
    {5702723040652442467ULL,  11325339470689059424ULL},
    {870698890980252409ULL,   8294946103885186165ULL},
    {423348447487367835ULL,   4067674294039261619ULL},
    {397951862030142664ULL,   17073640849499096681ULL},
    {9374556141781683538ULL,  10333311062251856416ULL},
    {1097707041202763764ULL,  2870200096551238743ULL},
    {11493051326088411054ULL, 12348796263566330575ULL},
    {15865059192259516415ULL, 4808544582161036476ULL},
    {2717981543414886593ULL,  5944564527643476706ULL},
    {887521262173735642ULL,   3558550013200985442ULL},
    {9496424291456600748ULL,  9845949835154361896ULL},
    {1589012859535948937ULL,  7402826160257180747ULL}
};

BOOST_AUTO_TEST_CASE(test_hash_output) {
    auto assert_hashes_equal = [] (bytes_view data, std::array<uint64_t,2> lhs, std::array<uint64_t,2> rhs) {
        if (lhs != rhs) {
            BOOST_FAIL(format("Hashes differ for {} (got {{0x{:x}, 0x{:x}}} and {{0x{:x}, 0x{:x}}})", to_hex(data),
                lhs[0], lhs[1], rhs[0], rhs[1]));
        }
    };

    for (size_t i = 0; i < full_sequence.size(); ++i) {
        auto prefix = bytes_view(full_sequence.begin(), i);
        auto&& expected = prefix_hashes[i];

        {
            std::array<uint64_t, 2> dst;
            utils::murmur_hash::hash3_x64_128(prefix, seed, dst);
            assert_hashes_equal(prefix, dst, expected);
        }

        // Test the iterator version
        {
            std::array<uint64_t,2> dst;
            utils::murmur_hash::hash3_x64_128(prefix.begin(), prefix.size(), seed, dst);
            assert_hashes_equal(prefix, dst, expected);
        }
    }
}
