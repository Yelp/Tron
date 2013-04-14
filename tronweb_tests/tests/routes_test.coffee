

describe "routes.coffee", ->
    module = window.modules.routes

    it "splitKeyValuePairs should create object from list", ->
        obj = module.splitKeyValuePairs ['one=two', 'three=four']
        expect(obj).toEqual
            one: 'two'
            three: 'four'

    it "getParamsMap should create object from string", ->
        obj = module.getParamsMap "a=nameThing;b=other"
        expect(obj).toEqual
            a: 'nameThing'
            b: 'other'


    # TODO: how to mock document.location
    xit "getLocationParams should return location pair", ->
        [base, params] = module.getLocationParams()
        expect(base).toEqual("#base")
        expect(params).toEqual
             one: "thing"
             another: "what"

    it "buildLocationString creates a location string", ->
        params =
            thing: "ok"
            bar: "tmp"
        location = module.buildLocationString "#base", params
        expect(location).toEqual("#base;thing=ok;bar=tmp")

    #TODO: also requires mocking document.location
    xdescribe "updateLocationParam", ->

        it "creates params when params is empty", ->

        it "updates existing param",  ->

        it "adds new params", ->
