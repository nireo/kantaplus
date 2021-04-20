//
// Created by eemil on 4/20/21.
//

#ifndef KANTAPLUS_RBTREE_HPP
#define KANTAPLUS_RBTREE_HPP

enum Color {
    Black
    Red
};

struct KVPair {
    std::string key;
    std::string value;
};

struct Node {
    KVPair data;
    Color color;

    Node *parent;
    Node *left;
    Node *right;
};

constexpr inline KVPair default_data = {
        .key = "",
        .value = "",
};

class RBTree {
    Node *root;
    Node *t_null;

    void init_null(Node *node, Node *parent) {
        node->data = default_data;
        node->parent = parent;

    }

public:

};

#endif //KANTAPLUS_RBTREE_HPP
