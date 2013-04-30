
module = modules.timeline

describe "Timeline module", ->

    it "padMaxDate adds padding to maxDate", ->
        dates = [new Date("2013-04-20 01:00:00"),
                 new Date("2013-04-20 02:30:30")]
        padded = module.padMaxDate(dates, 0.1)
        expect(padded).toEqual([dates[0], new Date("2013-04-20 02:39:33")])
