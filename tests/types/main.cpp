#if 0

Type t = TypeBuilder().Build();

Block b = Block::Create([t]);

#endif

class TypeBuilder {
public:
    virtual ~TypeBuilder() = default;
};

class ArrayTypeBuilder : public TypeBuilder {
    // Item
};

class TupleTypeBuilder : public TypeBuilder {
public:
    virtual TupleTypeBuilder& Add() = 0;
};

int main() {
    return 0;
}
