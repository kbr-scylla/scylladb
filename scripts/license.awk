BEGIN {
    replacing = 0
    blurb = "See the LICENSE.PROPRIETARY file in the top-level directory for licensing information."
}
/Scylla is free software/ {
    replacing = 1
}
{
    if (!replacing) {
	print
    }
}
/along with Scylla/ {
    print(gensub(/^( ?..).*$/, "\\1" blurb, 1))
    replacing = 0
}
