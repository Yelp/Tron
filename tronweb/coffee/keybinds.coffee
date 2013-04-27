
# Keybinds
window.modules = window.modules || {}
module = window.modules.keybinds = {}

isInputField = (event) ->
    event.target.tagName.toLowerCase() in ['input', 'textarea']


# TODO: docs
setFocusBinds = (el) ->
    el.bind 'keydown', (event) ->
        return if isInputField(event)
        #console.log(event.keyCode)
        switch event.keyCode
            when 70 then $('#view-full-screen').click()  # f
            when 82 then $('.refresh-view .btn').click() # r
            when 83 then $('.search-query').focus()      # s

            else return

        event.preventDefault()


$(document).ready ->
    setFocusBinds($(document))
