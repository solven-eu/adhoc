# Pivotable

## Quick-Start

Run the main in:

    PivotableServerApplication

## Pivotable vs Adhoc

While: 
- Adhoc is a query-engine, knowing how to execute queries to return views
- Pivotable is a client, managing a set of Adhoc endpoints, and providing an example JS application to show a view as a grid
- Pivotable can embed an Adhoc endpoint (named `self`), or query remote Adhoc endpoints.

## Main concepts

*Pivotable* is a web application composed of a back-end and a front-end.

The back-end:

- Serves API for the front-end.

The front-end:

- is a Single-Page-Application coded in Vue3

Pivotable should generally be considered as a reference implementation, and not as the privileged client for Adhoc queries.

## Migration

Being a reference implementation, we focus:

- more into a lean implementation, making it easier to understand existing concepts
- less into a retro-compatible application, making migrations a smooth-less effort.

## APIs core concepts

Pivotable enables:

- managing a set of *Adhoc Endpoints*. **To be defined**
- querying *Endpoints* schema, *Endpoints Cubes schemas* and *Endpoints Cubes Columns*.
- Endpoints are identified by a UUID. They also have a name and a URL.
- Cubes are identified by a human-friendly String.

## SPA core concepts

Pivotable enables:

- Listing entrypoints
- Showing entrypoints schemas, especially the available cubes
- Showing cubes schemas, especially the available columns and measures
- Search through columns available members

The default implementations is based over:

- Vue3 as a module
- Does not require compilation
- Enable quick developments
- Make not be satisfactory for production application (as it is sub-optimal by not being compiled)
- Make not be satisfactory for a full-fledged application (as it has strong restrictions in term of available tools (no TypeScript, no `.vue` files, etc)).

# Authentication

Pivotable authentication relies on a custom JWT. The JWT is generated following a login flow.

The signature of JWTs is managed by `PivotableResourceServerConfiguration`.
The generation of JWTs is managed by `PivotableTokenService`.

## Custom Authentication

The main class to define authentication methods is `PivotableLoginController`.

Pivotable provides 2 authentication methods by default:
- BASIC, through the Spring profile `fakeuser`
- OAuth2, with examples provided for Google and Github, relying on core [SpringBoot](https://docs.spring.io/spring-security/reference/servlet/oauth2/login/core.html).

One can register an additional oauth2 method with:

```
spring:
    application.name: pivotable-server
    security:
        oauth2:
            client:
                registration:
                    some_provider_id:
                        clientId: someClientId
                        clientSecret: someClientSecret
```