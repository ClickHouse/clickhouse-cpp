#include "type_parser.h"
#include "../base/string_utils.h"

#include <iostream>

namespace clickhouse {

static TypeAst::Meta GetTypeMeta(const StringView& name) {
    if (name == "Array") {
        return TypeAst::Array;
    }

    if (name == "Null") {
        return TypeAst::Null;
    }

    if (name == "Nullable") {
        return TypeAst::Nullable;
    }

    if (name == "Tuple") {
        return TypeAst::Tuple;
    }

    if (name == "Enum8" || name == "Enum16") {
        return TypeAst::Enum;
    }

    return TypeAst::Terminal;
}


TypeParser::TypeParser(const StringView& name)
    : cur_(name.data())
    , end_(name.data() + name.size())
    , type_(nullptr)
{
}

TypeParser::~TypeParser() = default;

bool TypeParser::Parse(TypeAst* type) {
    type_ = type;
    open_elements_.push(type_);

    do {
        const Token& token = NextToken();

        switch (token.type) {
            case Token::Name:
                type_->meta = GetTypeMeta(token.value);
                type_->name = token.value;
                break;
            case Token::Number:
                type_->meta = TypeAst::Number;
                type_->value = FromString<int>(token.value);
                break;
            case Token::LPar:
                type_->elements.emplace_back(TypeAst());
                open_elements_.push(type_);
                type_ = &type_->elements.back();
                break;
            case Token::RPar:
                type_ = open_elements_.top();
                open_elements_.pop();
                break;
            case Token::Comma:
                type_ = open_elements_.top();
                open_elements_.pop();
                type_->elements.emplace_back(TypeAst());
                open_elements_.push(type_);
                type_ = &type_->elements.back();
                break;
            case Token::EOS:
                return true;
            case Token::Invalid:
                return false;
        }
    } while (true);
}

TypeParser::Token TypeParser::NextToken() {
    for (; cur_ < end_; ++cur_) {
        switch (*cur_) {
            case ' ':
            case '\n':
            case '\t':
            case '\0':
                continue;

            case '=':
            case '\'':
                continue;

            case '(':
                return Token{Token::LPar, StringView(cur_++, 1)};
            case ')':
                return Token{Token::RPar, StringView(cur_++, 1)};
            case ',':
                return Token{Token::Comma, StringView(cur_++, 1)};

            default: {
                const char* st = cur_;

                if (isalpha(*cur_) || *cur_ == '_') {
                    for (; cur_ < end_; ++cur_) {
                        if (!isalpha(*cur_) && !isdigit(*cur_) && *cur_ != '_') {
                            break;
                        }
                    }

                    return Token{Token::Name, StringView(st, cur_)};
                }

                if (isdigit(*cur_) || *cur_ == '-') {
                    for (++cur_; cur_ < end_; ++cur_) {
                        if (!isdigit(*cur_)) {
                            break;
                        }
                    }

                    return Token{Token::Number, StringView(st, cur_)};
                }

                return Token{Token::Invalid, StringView()};
            }
        }
    }

    return Token{Token::EOS, StringView()};
}

}
