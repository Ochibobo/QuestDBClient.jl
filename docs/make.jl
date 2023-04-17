push!(LOAD_PATH,"../src/")
using Documenter
using QuestDBClient

makedocs(
    sitename = "QuestDBClient",
    format = Documenter.HTML(),
    modules = [QuestDBClient],
    pages = Any[
        "Home" => "index.md"
        "User Guide" => Any[
            "Functional Approach" => "man/functional.md"
            "Macro Approach" => "man/macros.md"
        ]
        "API" => Any[
            "Sender" => "lib/sender.md",
            "Operators" => "lib/operators.md",
            "Types" => "lib/types.md",
            "Exceptions" => "lib/exceptions.md"
        ]
    ]
)

# Documenter can also automatically deploy documentation to gh-pages.
# See "Hosting Documentation" and deploydocs() in the Documenter manual
# for more information.
#=deploydocs(
    repo = "<repository url>"
)=#
