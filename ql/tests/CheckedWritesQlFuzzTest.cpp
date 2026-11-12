// Checked Writes Part 08, Task 3: grammar round-trip model check.  Random statements are rendered to text, parsed, and
// compared with what was generated; random token soup and over-deep / over-long check predicates must error, never crash.
#include <rune/ql/Parser.h>
#include <gtest/gtest.h>
#include <cstdint>
#include <random>
#include <string>
#include <vector>

namespace ql = rune::ql;

namespace {

// Words the grammar treats as keywords somewhere: every one of them is still a legal column / view / alias name.
const std::vector<std::string> kNames = {"a", "b", "userId", "from", "key", "check", "unique", "where", "limit", "select", "group",
                                         "count", "min", "last", "lower", "constraint", "view", "create", "show", "and", "or"};

struct Gen {
    std::mt19937_64 rng;
    explicit Gen(uint64_t seed) : rng(seed) {}
    std::size_t below(std::size_t n) { return static_cast<std::size_t>(rng() % n); }
    bool coin() { return rng() & 1; }
    std::string name() { return kNames[below(kNames.size())]; }

    ql::Literal literal(bool allowLower) {
        ql::Literal l;
        switch (below(3)) {
        case 0: l.kind = ql::Literal::Kind::Integer; l.negative = coin(); l.magnitude = rng() >> below(64);
                if (l.negative && l.magnitude > (1ull << 63)) l.magnitude >>= 1;
                break;
        case 1: l.kind = ql::Literal::Kind::String; l.text = name() + std::to_string(below(100));
                l.lowered = allowLower && coin();
                break;
        default: l.kind = ql::Literal::Kind::Bool; l.boolean = coin(); break;
        }
        return l;
    }

    // Children are pushed before their parent, root last (the parser's order).
    uint16_t pred(std::vector<ql::PredNode>& out, int depth) {
        const std::size_t pick = depth <= 0 ? 0 : below(4);
        if (pick == 0) {
            static constexpr ql::PredOp ops[] = {ql::PredOp::Eq, ql::PredOp::Ne, ql::PredOp::Lt, ql::PredOp::Le, ql::PredOp::Gt, ql::PredOp::Ge};
            std::string col = name();
            const uint16_t a = push(out, {ql::PredOp::Column, 0, 0, col, {}});
            const uint16_t b = push(out, {ql::PredOp::Const, 0, 0, {}, literal(false)});
            return push(out, {ops[below(6)], a, b, {}, {}});
        }
        if (pick == 1) return push(out, {ql::PredOp::Not, pred(out, depth - 1), 0, {}, {}});
        const uint16_t l = pred(out, depth - 1);
        const uint16_t r = pred(out, depth - 1);
        return push(out, {pick == 2 ? ql::PredOp::And : ql::PredOp::Or, l, r, {}, {}});
    }
    static uint16_t push(std::vector<ql::PredNode>& out, ql::PredNode n) {
        out.push_back(std::move(n));
        return static_cast<uint16_t>(out.size() - 1);
    }

    ql::KeyItem keyItem(bool group) {
        ql::KeyItem k;
        k.column = name();
        if (coin()) { k.fn = ql::KeyFunction::Lower; k.outName = group ? std::string() : name(); }
        else k.outName = k.column;
        return k;
    }

    ql::Statement createView() {
        ql::CreateViewDef v;
        v.name = name();
        v.orReplace = coin();
        v.transient = coin();
        v.unique = coin();
        for (std::size_t i = 0, n = 1 + below(3); i < n; ++i) v.key.push_back(keyItem(false));
        if (below(5) == 0) v.items.push_back({ql::ItemKind::Star, {}, {}});
        else for (std::size_t i = 0, n = 1 + below(4); i < n; ++i) {
            static constexpr ql::ItemKind aggs[] = {ql::ItemKind::Count, ql::ItemKind::Sum, ql::ItemKind::Avg, ql::ItemKind::Min, ql::ItemKind::Max, ql::ItemKind::Last};
            switch (below(3)) {
            case 0: v.items.push_back({ql::ItemKind::Column, name(), coin() ? name() : std::string()}); break;
            case 1: v.items.push_back({aggs[below(6)], name(), name()}); break;
            default: v.items.push_back({ql::ItemKind::CountAll, {}, name()}); break;
            }
        }
        v.source = name();
        for (std::size_t i = 0, n = below(3); i < n; ++i) {
            ql::ConstraintDef c;
            c.name = name();
            pred(c.predicate, static_cast<int>(below(5)));
            v.constraints.push_back(std::move(c));
        }
        return v;
    }

    ql::Statement select() {
        ql::SelectDef s;
        for (std::size_t i = 0, n = below(4); i < n; ++i) s.columns.push_back(name());
        s.view = name();
        std::vector<std::string> used;
        for (std::size_t i = 0, n = 1 + below(3); i < n; ++i) {
            std::string c = name();
            bool dup = false;
            for (const auto& u : used) dup = dup || u == c;
            if (dup) continue;
            used.push_back(c);
            s.where.push_back({c, literal(true)});
        }
        if (coin()) s.limit = static_cast<uint32_t>(1 + rng() % 4294967295ull);
        return s;
    }

    ql::Statement statement() {
        switch (below(4)) {
        case 0: return createView();
        case 1: return ql::ShowTopicDef{name()};
        case 2: return ql::ShowViewDef{name()};
        default: return select();
        }
    }
};

std::string lit(const ql::Literal& l) {
    switch (l.kind) {
    case ql::Literal::Kind::Integer: return (l.negative ? "-" : "") + std::to_string(l.magnitude);
    case ql::Literal::Kind::String: return l.lowered ? "lower('" + l.text + "')" : "'" + l.text + "'";
    default: return l.boolean ? "true" : "false";
    }
}

std::string pred(const std::vector<ql::PredNode>& p, uint16_t i) {
    const auto& n = p[i];
    switch (n.op) {
    case ql::PredOp::Not: return "not " + pred(p, n.a);
    case ql::PredOp::And: return "(" + pred(p, n.a) + " and " + pred(p, n.b) + ")";
    case ql::PredOp::Or:  return "(" + pred(p, n.a) + " or " + pred(p, n.b) + ")";
    default: {
        const char* op = n.op == ql::PredOp::Eq ? "=" : n.op == ql::PredOp::Ne ? "<>" : n.op == ql::PredOp::Lt ? "<" :
                         n.op == ql::PredOp::Le ? "<=" : n.op == ql::PredOp::Gt ? ">" : ">=";
        return p[n.a].column + " " + op + " " + lit(p[n.b].value);
    }
    }
}

std::string keyItem(const ql::KeyItem& k, bool group) {
    if (k.fn == ql::KeyFunction::None) return k.column;
    return "lower(" + k.column + ")" + (group ? "" : " as " + k.outName);
}

std::string render(const ql::Statement& st) {
    if (const auto* t = std::get_if<ql::ShowTopicDef>(&st)) return "show topic " + t->name + ";";
    if (const auto* t = std::get_if<ql::ShowViewDef>(&st)) return "show view " + t->name + ";";
    if (const auto* s = std::get_if<ql::SelectDef>(&st)) {
        std::string o = "select ";
        if (s->columns.empty()) o += "*";
        for (std::size_t i = 0; i < s->columns.size(); ++i) o += (i ? ", " : "") + s->columns[i];
        o += " from " + s->view + " where ";
        for (std::size_t i = 0; i < s->where.size(); ++i) o += (i ? " and " : "") + s->where[i].column + " = " + lit(s->where[i].value);
        if (s->limit) o += " limit " + std::to_string(*s->limit);
        return o + ";";
    }
    const auto& v = std::get<ql::CreateViewDef>(st);
    std::string o = "create ";
    if (v.orReplace) o += "or replace ";
    if (v.transient) o += "transient ";
    if (v.unique) o += "unique ";
    o += "view " + v.name + " key (";
    for (std::size_t i = 0; i < v.key.size(); ++i) o += (i ? ", " : "") + keyItem(v.key[i], false);
    o += ") as select ";
    for (std::size_t i = 0; i < v.items.size(); ++i) {
        const auto& it = v.items[i];
        o += i ? ", " : "";
        static const char* const fn[] = {"", "", "", "count", "sum", "avg", "min", "max", "last"};
        switch (it.kind) {
        case ql::ItemKind::Star: o += "*"; break;
        case ql::ItemKind::Column: o += it.column + (it.alias.empty() ? "" : " as " + it.alias); break;
        case ql::ItemKind::CountAll: o += "count(*) as " + it.alias; break;
        default: o += std::string(fn[static_cast<int>(it.kind)]) + "(" + it.column + ") as " + it.alias; break;
        }
    }
    o += " from " + v.source;
    for (const auto& c : v.constraints) o += " constraint " + c.name + " check (" + pred(c.predicate, static_cast<uint16_t>(c.predicate.size() - 1)) + ")";
    return o + ";";
}

}  // namespace

TEST(CheckedWritesQlFuzzRegression, GeneratedStatementsRoundTrip) {
    Gen g(0x5eedc0de);
    for (int i = 0; i < 3000; ++i) {
        const ql::Statement st = g.statement();
        const std::string text = render(st);
        auto r = ql::parse(text);
        ASSERT_TRUE(r.has_value()) << text << " -> " << r.error().message;
        ASSERT_EQ(r->index(), st.index()) << text;
        EXPECT_EQ(render(*r), text);
        if (const auto* s = std::get_if<ql::SelectDef>(&st)) {
            const auto& p = std::get<ql::SelectDef>(*r);
            EXPECT_EQ(p.columns, s->columns) << text;
            EXPECT_EQ(p.view, s->view) << text;
            EXPECT_EQ(p.limit, s->limit) << text;
            ASSERT_EQ(p.where.size(), s->where.size()) << text;
            for (std::size_t k = 0; k < p.where.size(); ++k) {
                EXPECT_EQ(p.where[k].column, s->where[k].column) << text;
                EXPECT_EQ(p.where[k].value.kind, s->where[k].value.kind) << text;
                EXPECT_EQ(p.where[k].value.negative, s->where[k].value.negative) << text;
                EXPECT_EQ(p.where[k].value.magnitude, s->where[k].value.magnitude) << text;
                EXPECT_EQ(p.where[k].value.text, s->where[k].value.text) << text;
                EXPECT_EQ(p.where[k].value.lowered, s->where[k].value.lowered) << text;
            }
        }
    }
}

TEST(CheckedWritesQlFuzzRegression, RandomTokensNeverCrash) {
    static const std::vector<std::string> kTokens = {
        "select", "show", "topic", "view", "create", "or", "replace", "unique", "transient", "key", "as", "from", "where", "limit",
        "group", "by", "constraint", "check", "and", "not", "lower", "count", "last", "of", "version", "schema", "alter", "drop",
        "grant", "revoke", "x", "y", "42", "-7", "'s'", "'unterminated", "(", ")", ",", ";", "*", "=", "<>", "<=", ">=", "<", ">",
        "-", ".", "\xff", "99999999999999999999999"};
    Gen g(0xf00dfeed);
    for (int i = 0; i < 5000; ++i) {
        std::string text;
        for (std::size_t n = 1 + g.below(24); n > 0; --n) text += kTokens[g.below(kTokens.size())] + " ";
        (void)ql::parse(text);
        (void)ql::parseScript(text);
    }
}

TEST(CheckedWritesQlFuzzRegression, DeepAndLongPredicatesErrorNeverCrash) {
    const auto view = [](const std::string& check) { return "create view v key k as select k from t constraint c check (" + check + ");"; };
    const auto nested = [](int depth, const std::string& open, const std::string& close) {
        std::string s;
        for (int i = 0; i < depth; ++i) s += open;
        s += "x = 1";
        for (int i = 0; i < depth; ++i) s += close;
        return s;
    };
    EXPECT_TRUE(ql::parse(view(nested(20, "(", ")"))).has_value());
    EXPECT_TRUE(ql::parse(view(nested(20, "not ", ""))).has_value());
    for (int depth : {200, 5000, 100000}) {
        EXPECT_FALSE(ql::parse(view(nested(depth, "(", ")"))).has_value()) << depth;
        EXPECT_FALSE(ql::parse(view(nested(depth, "not ", ""))).has_value()) << depth;
    }
    std::string chain = "x = 1";
    for (int i = 0; i < 30000; ++i) chain += " and x = 1";
    EXPECT_FALSE(ql::parse(view(chain)).has_value());
}
