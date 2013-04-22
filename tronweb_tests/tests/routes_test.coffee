

describe "routes.coffee", ->
    module = window.modules.routes

    it "splitKeyValuePairs creates object from list", ->
        obj = module.splitKeyValuePairs ['one=two', 'three=four']
        expect(obj).toEqual
            one: 'two'
            three: 'four'

    it "getParamsMap creates object from string", ->
        obj = module.getParamsMap "a=nameThing;b=other"
        expect(obj).toEqual
            a: 'nameThing'
            b: 'other'


    describe "getLocationParams", ->

        beforeEach ->
            spyOn(module, 'getLocationHash')

        it "returns location with params", ->
            location = "#base;one=thing;another=what"
            module.getLocationHash.andReturn(location)
            [base, params] = module.getLocationParams()
            expect(base).toEqual("#base")
            expect(params).toEqual
                 one: "thing"
                 another: "what"

        it "returns location without params", ->
            module.getLocationHash.andReturn("#blah")
            [base, params] = module.getLocationParams()
            expect(base).toEqual("#blah")
            expect(params).toEqual {}


    it "buildLocationString creates a location string", ->
        params =
            thing: "ok"
            bar: "tmp"
        location = module.buildLocationString "#base", params
        expect(location).toEqual("#base;thing=ok;bar=tmp")


    describe "updateLocationParam", ->

        beforeEach ->
            window.routes = jasmine.createSpyObj('routes', ['navigate'])
            spyOn(module, 'getLocationHash')

        it "creates params when params is empty", ->
            module.getLocationHash.andReturn("#base")
            module.updateLocationParam('name', 'stars')
            expected = "#base;name=stars"
            expect(window.routes.navigate).toHaveBeenCalledWith(expected)

        it "updates existing param",  ->
            module.getLocationHash.andReturn("#base;name=foo")
            module.updateLocationParam('name', 'stars')
            expected = "#base;name=stars"
            expect(window.routes.navigate).toHaveBeenCalledWith(expected)

        it "adds new params", ->
            module.getLocationHash.andReturn("#base;what=why")
            module.updateLocationParam('name', 'stars')
            expected = "#base;what=why;name=stars"
            expect(window.routes.navigate).toHaveBeenCalledWith(expected)
