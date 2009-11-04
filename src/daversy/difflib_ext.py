import re, difflib

def merge_group(list, func, start=True, end=True):
    l, r, s = list[0]
    first = ['',' class="first"'][start]
    last  = ['',' class="last"'][end]

    if len(list) == 1:
        if start and end:
            return LINE_FORMAT % func(' class="first last"', l, r)
        else:
            return LINE_FORMAT % func(first+last, l, r)

    html = LINE_FORMAT % func(first, l, r)

    for i in range(1, len(list)-1):
        l, r, s = list[i]
        html += LINE_FORMAT % func('', l, r)

    l, r, s = list[-1]
    html += LINE_FORMAT % func(last, l, r)
    return html


def make_table(table_id, header, fromlines, tolines, context=None, versions=['old', 'new']):
    diff = list(difflib._mdiff(fromlines, tolines, context))
    if not diff:
        return None

    same = lambda c, l, r: (c, l[0], r[0], 'l', format_line(l[1]))
    add  = lambda c, l, r: (c, '',   r[0], 'r', format_line(r[1]))
    sub  = lambda c, l, r: (c, l[0], '',   'l', format_line(l[1]))

    html = TABLE_HEADER % tuple([table_id, header] + versions)
    for type, start, end in group_types(diff):
        if type == 'same':
            html += '<tbody>%s</tbody>\n' % \
                    merge_group(diff[start:end], same)

        elif type == 'add':
            html += '<tbody class="add">%s</tbody>\n' % \
                    merge_group(diff[start:end], add)

        elif type == 'del':
            html += '<tbody class="rem">%s</tbody>\n' % \
                    merge_group(diff[start:end], sub)

        elif type == 'mod':
            html += '<tbody class="mod">%s%s</tbody>\n' % \
                    (merge_group(diff[start:end], sub, end=False),
                     merge_group(diff[start:end], add, start=False))
        elif type == 'skipped':
            html += '<tbody class="skipped"><tr><th>...</th><th>...</th><td>&nbsp;</td></tr></tbody>\n'
    html += TABLE_FOOTER
    return html

def get_type(left, right, status):
    if not status:
        if left or right:
            return 'same'
        else:
            return 'skipped'

    l_num, l_line = left
    r_num, r_line = right
    if  l_num and not r_num:
        return 'del'
    elif r_num and not l_num:
        return 'add'
    else:
        return 'mod'

def group_types(diff):
    items = [get_type(l,r,s) for l,r,s in diff]
    group = []

    if not items:
        print diff

    start, current = 0, items[0]
    for i in range(1, len(diff)):
        if items[i] != current:
            group.append( (current, start, i) )
            current = items[i]
            start   = i
    group.append( (current, start, len(diff)) )
    return group

REPLACE_CHARS = [
 ('&',   '&amp;'),
 ('<',   '&lt;'),
 ('>',   '&gt;'),
 (' ',   '&nbsp;'),
 ('"',   '&quot;'),
 ('\0+', '<span class="ins">'),
 ('\0-', '<span class="del">'),
 ('\0^', '<span class="chg">'),
 ('\1',  '</span>')
]

SINGLE_CHANGE = re.compile("^\0[\+\-\^]([^\0]+)\1\n?$")

def format_line(text):
    text = text.replace('\n', '')
    match = SINGLE_CHANGE.match(text)
    if match:
        text = match.group(1)

    for src, replace in REPLACE_CHARS:
        text = text.replace(src, replace)

    return text

## the majority of the CSS and markup has been used from Trac

TABLE_HEADER = """
 <li class='entry' id='%s'>
   <h2>%s</h2>
   <table class="inline" summary="Differences" cellspacing="0">
     <colgroup><col class="lineno" /><col class="lineno" /><col class="content" /></colgroup>
     <thead><th>%s</th><th>%s</th><th>&nbsp;</th></thead>
"""

TABLE_FOOTER = """
   </table>
 </li>
"""

LINE_FORMAT = "<tr%s><th>%s</th><th>%s</th><td class='%s'><span>%s</span>&nbsp;</td></tr>"

HTML_HEADER = """
<html><head><style type='text/css'>
/* Diff preferences */
#prefs fieldset { margin: 1em .5em .5em; padding: .5em 1em 0 }

/* Diff/change overview */
#overview {
 line-height: 130%;
 margin-top: 1em;
 padding: .5em;
}
#overview dt {
 font-weight: bold;
 padding-right: .25em;
 position: absolute;
 left: 0;
 text-align: right;
 width: 7.75em;
}
#overview dd { margin-left: 8em }

/* Colors for change types */
#chglist .edit, #overview .mod, .diff #legend .mod { background: #fd8 }
#chglist .delete, #overview .rem, .diff #legend .rem { background: #f88 }
#chglist .add, #overview .add, .diff #legend .add { background: #bfb }
#chglist .copy, #overview .cp, .diff #legend .cp { background: #88f }
#chglist .move, #overview .mv, .diff #legend .mv { background: #ccc }
#chglist .unknown { background: #fff }

/* Legend for diff colors */
.diff #legend {
 float: left;
 font-size: 9px;
 line-height: 1em;
 margin: 1em 0;
 padding: .5em;
}
.diff #legend h3 { display: none; }
.diff #legend dt {
 background: #fff;
 border: 1px solid #999;
 float: left;
 margin: .1em .5em .1em 2em;
 overflow: hidden;
 width: .8em; height: .8em;
}
.diff #legend dl, .diff #legend dd {
 display: inline;
 float: left;
 padding: 0;
 margin: 0;
 margin-right: .5em;
}

/* Styles for the list of diffs */
.diff ul.entries { clear: both; margin: 0; padding: 0 }
.diff li.entry {
 background: #f7f7f7;
 border: 1px solid #d7d7d7;
 list-style-type: none;
 margin: 0 0 2em;
 padding: 2px;
 position: relative;
}
.diff h2 {
 color: #333;
 font-size: 14px;
 letter-spacing: normal;
 margin: 0 auto;
 padding: .1em 0 .25em .5em;
}

/* Styles for the actual diff tables (side-by-side and inline) */
.diff table {
 border: 1px solid #ddd;
 border-spacing: 0;
 border-top: 0;
 empty-cells: show;
 font-size: 12px;
 line-height: 130%;
 padding: 0;
 margin: 0 auto;
 width: 100%;
}
.diff table col.lineno { width: 4em }
.diff table th {
 border-right: 1px solid #d7d7d7;
 border-bottom: 1px solid #998;
 font-size: 11px;
}
.diff table thead th {
 background: #eee;
 border-top: 1px solid #d7d7d7;
 color: #999;
 padding: 0 .25em;
 text-align: center;
 white-space: nowrap;
}
.diff table tbody th {
 background: #eed;
 color: #886;
 font-weight: normal;
 padding: 0 .5em;
 text-align: right;
 vertical-align: top;
}
.diff table tbody td {
 background: #fff;
 font: normal 11px monospace;
 overflow: hidden;
 padding: 1px 2px;
 vertical-align: top;
}
.diff table tbody.skipped td {
 background: #f7f7f7;
 border: 1px solid #d7d7d7;
}
.diff table td span.del, .diff table td span.ins { text-decoration: none }
.diff table td span.del { color: #600 }
.diff table td span.ins { color: #060 }

/* Styles for the inline diff */
.diff table.inline tbody.mod td.l, .diff table.inline tbody.rem td.l {
 background: #fdd;
 border-color: #c00;
 border-style: solid;
 border-width: 0 1px 0 1px;
}
.diff table.inline tbody.mod td.r, .diff table.inline tbody.add td.r {
 background: #dfd;
 border-color: #0a0;
 border-style: solid;
 border-width: 0 1px 0 1px;
}
.diff table.inline tbody.mod tr.first td.l,
.diff table.inline tbody.rem tr.first td.l { border-top-width: 1px }
.diff table.inline tbody.mod tr.last td.l,
.diff table.inline tbody.rem tr.last td.l { border-bottom-width: 1px }
.diff table.inline tbody.mod tr.first td.r,
.diff table.inline tbody.add tr.first td.r { border-top-width: 1px }
.diff table.inline tbody.mod tr.last td.r,
.diff table.inline tbody.add tr.last td.r { border-bottom-width: 1px }
.diff table.inline tbody.mod td span.del { background: #e99; color: #000 }
.diff table.inline tbody.mod td span.ins { background: #9e9; color: #000 }
.diff table.inline tbody.mod td span.chg { background: #ee9; color: #000 }

/* Styles for the side-by-side diff */
.diff table.sidebyside colgroup.content { width: 50% }
.diff table.sidebyside tbody.mod td.l { background: #fe9 }
.diff table.sidebyside tbody.mod td.r { background: #fd8 }
.diff table.sidebyside tbody.add td.l { background: #dfd }
.diff table.sidebyside tbody.add td.r { background: #cfc }
.diff table.sidebyside tbody.rem td.l { background: #f88 }
.diff table.sidebyside tbody.rem td.r { background: #faa }
.diff table.sidebyside tbody.mod span.del, .diff table.sidebyside tbody.mod span.ins, .diff table.sidebyside tbody.mod span.chg {
 background: #fc0;
}
/* Changeset overview */
#overview .files { padding-top: 2em }
#overview .files ul { margin: 0; padding: 0 }
#overview .files li { list-style-type: none }
#overview .files li .comment { display: none }
#overview .files li div {
 border: 1px solid #999;
 float: left;
 margin: .2em .5em 0 0;
 overflow: hidden;
 width: .8em; height: .8em;
}
#overview div.add div, #overview div.cp div, #overview div.mv div {
 border: 0;
 margin: 0;
 float: right;
 width: .35em;
}

span.ver {font: normal 11px monospace;}
</style></head><body>
"""

HTML_FOOTER = """
 </body>
</html>
"""
