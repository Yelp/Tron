

describe "Job module", ->

    jobsFor = (teams) ->
        (new Job(name: "job#{i}", monitoring: {team: t}) for t, i in teams)

    describe "jobTeam", ->
        it "returns the monitoring team", ->
            job = new Job(monitoring: {team: 'secidr'})
            expect(jobTeam(job)).toEqual('secidr')

        it "returns empty string without monitoring", ->
            expect(jobTeam(new Job())).toEqual('')

    describe "uniqueTeams", ->
        it "returns sorted unique teams, skipping jobs without one", ->
            jobs = jobsFor(['b', 'a', 'b']).concat([new Job()])
            expect(uniqueTeams(jobs)).toEqual(['a', 'b'])

    describe "team filter", ->
        it "matches the team exactly", ->
            filter = new JobListFilterModel(teamFilter: 'payments').createFilter()
            jobs = jobsFor(['payments', 'payments-noncritical'])
            expect(jobTeam(job) for job in jobs when filter(job)).toEqual(['payments'])

    describe "team dropdown", ->
        render = (teamFilter, teams) ->
            view = new FilterView(model: new JobListFilterModel(teamFilter: teamFilter))
            view.teams = teams
            view.render()

        it "lists teams and selects the current one", ->
            view = render('b', ['a', 'b'])
            expect(view.$('#filter-team option').map(-> @value).get()).toEqual(['', 'a', 'b'])
            expect(view.$('#filter-team').val()).toEqual('b')

        it "keeps a team from the URL that no loaded job has", ->
            view = render('gone', ['a'])
            expect(view.$('#filter-team').val()).toEqual('gone')

        it "only shows the clear link when a team is selected", ->
            expect(render('', ['a']).$('.clear-filter').attr('style')).toContain('display: none')
            expect(render('a', ['a']).$('.clear-filter').attr('style')).toBeUndefined()
