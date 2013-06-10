

describe "actionrun.coffee", ->
    module = modules.actionrun

    describe "ActionRun Model", ->
        self = this

        beforeEach ->
            self.actionRun = new module.ActionRun
                action_name: 'action_name'
                job_name: 'job_name'
                run_num: 'run_num'

        it "url creates the correct url", ->
            url = self.actionRun.url()
            expect(url).toEqual('/jobs/job_name/run_num/action_name' +
                self.actionRun.urlArgs)

        it "parse builds urls", ->
            resp = self.actionRun.parse {}
            expect(resp['job_url']).toEqual('#job/job_name')
            expect(resp['job_run_url']).toEqual('#job/job_name/run_num')
            expect(resp['url']).toEqual('#job/job_name/run_num/action_name')

    describe "ActionRunHistory Model", ->
        self = this

        beforeEach ->
            self.collection = new module.ActionRunHistory [],
                job_name: 'job_name'
                action_name: 'action_name'

        it "url creates the correct url", ->
            expect(self.collection.url()).toEqual(
                '/jobs/job_name/action_name/')
