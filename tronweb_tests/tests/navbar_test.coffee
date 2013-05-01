
describe "navbar module", ->

    describe "NavView", ->

        describe "sorter", ->
            test = @

            beforeEach ->
                test.view = new modules.navbar.NavView()
                test.items = [
                    "three",
                    "one",
                    "one longer",
                    "TWO",
                    "a one",
                    "longone with TwO ones"]

            it "sorts shorter items first", ->
                mockThis = query: "one"
                sortedItems = test.view.sorter.call(mockThis, test.items)
                expected = [
                    "one",
                    "one longer",
                    "a one",
                    "longone with TwO ones"]
                expect(sortedItems).toEqual(expected)

            it "sorts only matching items", ->
                mockThis = query: "TWO"
                sortedItems = test.view.sorter.call(mockThis, test.items)
                expect(sortedItems).toEqual(["TWO", "longone with TwO ones"])

            it "matches case insensitive", ->
                mockThis = query: "two"
                sortedItems = test.view.sorter.call(mockThis, test.items)
                expect(sortedItems).toEqual(["TWO", "longone with TwO ones"])
