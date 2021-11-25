#pragma once

/**
 * Extremely basic in-place sax-style xml parser.
 * Unlike other sax parsers such as libxml, this parser hands out pointers to the original input data.
 * This makes it possible to parse xml files that are very large but still keep references to all their content.
 * The idea is inspired by RapidXML's parse_non_destructive mode but uses less RAM because it does not build a DOM.
 * Does not replace entities.
 */
class ipsx {
    public:
        struct Node {
            const char *pointer;
            size_t length;
        };

        const char *data;
        const char *end;

        explicit ipsx(const char *data, size_t size) : data(data), end(data + size) {
        }

        Node readElementStart() {
            do {
                skip<CharPred<'<'>>();
                data++;
            } while(*data == '/' && !hasEnded());
            Node element = {};
            element.pointer = data;
            skip<NodeNamePred>();
            element.length = data - element.pointer;
            skip<CharPred<'>'>>();
            data++;
            return element;
        }

        Node readElementStart(const char *name) {
            Node element = {};
            do {
                element = readElementStart();
            } while (memcmp(element.pointer, name, element.length) != 0);
            return element;
        }

        Node readTextContent() {
            Node element = {};
            element.pointer = data;
            skip<CharPred<'<'>>();
            element.length = data - element.pointer;
            return element;
        }

        [[nodiscard]] bool hasEnded() const {
            return data >= end;
        }
    private:
        template<class StopPred>
        void skip() {
            while (StopPred::test(*data) && !hasEnded()) {
                ++data;
            }
        }

        struct NodeNamePred {
            static bool test(char x) {
                return lookupNodeName[(size_t) x] == 1;
            }
        };

        template<char c>
        struct CharPred {
            static bool test(char x) {
                return x != c;
            }
        };

        // Node name (anything but space \n \r \t / > ? \0)
        static constexpr char lookupNodeName[256] = {
                // 0   1   2   3   4   5   6   7   8   9   A   B   C   D   E   F
                0,  1,  1,  1,  1,  1,  1,  1,  1,  0,  0,  1,  1,  0,  1,  1,  // 0
                1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  // 1
                0,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  0,  // 2
                1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  0,  0,  // 3
                1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  // 4
                1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  // 5
                1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  // 6
                1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  // 7
                1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  // 8
                1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  // 9
                1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  // A
                1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  // B
                1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  // C
                1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  // D
                1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  // E
                1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1   // F
        };
};
