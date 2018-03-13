/*
 * Copyright 2016 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

// TODO: test final types more

class simple_compound {
    uint32_t foo;
    uint32_t bar;
};

class writable_final_simple_compound final stub [[writable]] {
    uint32_t foo;
    uint32_t bar;
};

class writable_simple_compound stub [[writable]] {
    uint32_t foo;
    uint32_t bar;
};

struct wrapped_vector {
    std::vector<simple_compound> vector;
};

struct vectors_of_compounds {
    std::vector<simple_compound> first;
    wrapped_vector second;
};

struct writable_wrapped_vector stub [[writable]] {
    std::vector<simple_compound> vector;
};

struct writable_vectors_of_compounds stub [[writable]] {
    std::vector<writable_simple_compound> first;
    writable_wrapped_vector second;
};

struct writable_vector stub [[writable]] {
    std::vector<simple_compound> vector;
};


struct writable_variants stub [[writable]] {
    int id;
    boost::variant<writable_vector, simple_compound, writable_final_simple_compound> first;
    boost::variant<writable_vector, simple_compound, writable_final_simple_compound> second;
    boost::variant<writable_vector, simple_compound, writable_final_simple_compound> third;
};

struct compound_with_optional {
    std::experimental::optional<simple_compound> first;
    simple_compound second;
};

class non_final_composite_test_object {
    simple_compound x();
};

class final_composite_test_object final {
    simple_compound x();
};
