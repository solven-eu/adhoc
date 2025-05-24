When writing code, you MUST follow these principles:

- Code should be easy to read and understand.
- Keep the code as simple as possible. Avoid unnecessary complexity.
- Use meaningful names for variables, functions, etc. Names should reveal intent.
- Only use comments when necessary, as they can become outdated. Instead, strive to make the code self-explanatory.
- When comments are used, they should add useful information that is not readily apparent from the code itself.
- Properly handle errors and exceptions to ensure the software's robustness.
- Use exceptions rather than error codes for handling errors.

- Interfaces are prefixed with `I`
- Abstract classes are prefixed with `A`
- Enum values are Pascal-case.

- Generally prefer data-structures maintaining order (e.g. `LinkedHashSet` vs `HashSet`). Typically, beware of `.collect(Collectors.toSet())` which does not maintain order.
- Generally prefer Guava collections over JDK11 wrappers (e.g. `ImmutableList.of(...)` vs `List.of()`). Typically, `List.of().contains(null)` throws an `Exception`.