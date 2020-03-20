## Rewriting

Series names can be rewritten as they pass through the system by Rewriter rules, which are processed in series.

Basic rules use simple old/new text replacement, and support a Max parameter to specify the maximum number of matched items to be replaced.

Rewriter rules also support regexp syntax, which is enabled by wrapping the "old" parameter with forward slashes and setting "max" to -1.
The "new" value can include [submatch identifiers](https://golang.org/pkg/regexp/#Regexp.Expand) in the format `${1}`.

Note that for performance reasons, these regular expressions don't support lookaround (lookahead, lookforward)
For more information see:

* [this ticket](https://github.com/StefanSchroeder/Golang-Regex-Tutorial/issues/11) 
* [syntax documentation](https://github.com/google/re2/wiki/Syntax)
* [golang's regular expression package documentation](https://golang.org/pkg/regexp/syntax/)

## Examples

### Using the new config style

This is the recommended approach.
The new config style also supports an extra field: `not`: skips the rewriting if the metric matches the pattern.
The pattern can either be a substring, or a regex enclosed in forward slashes.

```
# basic rewriter rule to replace first occurrence of "foo" with "bar"
[[rewriter]]
old = 'foo'
new = 'bar'
not = ''
max = -1

# regexp rewriter rule to add a prefix of "prefix." to all series
[[rewriter]]
old = '/^/'
new = 'prefix.'
not = ''
max = -1

# regexp rewriter rule to replace "server.X" with "servers.X.collectd"
[[rewriter]]
old = '/server\.([^.]+)/'
new = 'servers.${1}.collectd'
not = ''
max = -1

# same, except don't do it if the metric already contains the word collectd
[[rewriter]]
old = '/server\.([^.]+)/'
new = 'servers.${1}.collectd'
not = 'collectd'
max = -1

# same, with duplicate on
[[rewriter]]
old = '/server\.([^.]+)/'
new = 'servers.${1}.collectd'
not = 'collectd'
max = -1
duplicate = true
```

### Duplicate

The duplicate feature allows the renamed datapoint to be duplicated and the new point will be routed instantly to prevent additionnal renaming (thus bypassing aggregation !), the original datapoint can then match additionnal rewriters. 

This feature is meant only for metadata route to copy aggregation naming without doing any aggregation

### Using init commands

(deprecated)

```
# basic rewriter rule to replace first occurrence of "foo" with "bar"
addRewriter foo bar 1

# regexp rewriter rule to add a prefix of "prefix." to all series
addRewriter /^/ prefix. -1

# regexp rewriter rule to replace "server.X" with "servers.X.collectd"
addRewriter /server\.([^.]+)/ servers.${1}.collectd -1
```

