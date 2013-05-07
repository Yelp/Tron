

describe "Dashboard module", ->

    describe "JobStatusBoxView", ->
        test = @

        beforeEach ->
            test.model = new Job()
            spyOn(test.model, 'get')
            test.view = new modules.dashboard.JobStatusBoxView(model: test.model)

        it "count an empty list", ->
            test.model.get.andReturn([])
            expect(test.view.count()).toEqual(0)

        it "count a non-empty list returns first items run number", ->
            runs = [{'run_num': 5}, {'run_num': 4}, {'run_num': 3}]
            test.model.get.andReturn(runs)
            expect(test.view.count()).toEqual(5)
