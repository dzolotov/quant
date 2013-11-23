function LiangBarsky(edgeLeft, edgeRight, edgeBottom, edgeTop,   // Define the x/y clipping values for the border.
                     x0src, y0src, x1src, y1src)                               // Define the start and end points of the line.
{

    t0 = 0.0;
    t1 = 1.0;
    xdelta = x1src - x0src;
    ydelta = y1src - y0src;

    var p, q, r;

    for (var edge = 0; edge < 4; edge++) {   // Traverse through left, right, bottom, top edges.
        if (edge == 0) {
            p = -xdelta;
            q = -(edgeLeft - x0src);
        }
        if (edge == 1) {
            p = xdelta;
            q = (edgeRight - x0src);
        }
        if (edge == 2) {
            p = -ydelta;
            q = -(edgeBottom - y0src);
        }
        if (edge == 3) {
            p = ydelta;
            q = (edgeTop - y0src);
        }
        r = q / p;

        if (p == 0 && q < 0) return false;   // Don't draw line at all. (parallel line outside)

        if (p < 0) {
            if (r > t1) return false;         // Don't draw line at all.
            else if (r > t0) t0 = r;            // Line is clipped!
        } else if (p > 0) {
            if (r < t0) return false;      // Don't draw line at all.
            else if (r < t1) t1 = r;         // Line is clipped!
        }
    }

    box = new Object();
    box.lineLeft = x0src + t0 * xdelta;
    box.lineTop = y0src + t0 * ydelta;
    box.lineRight = x0src + t1 * xdelta;
    box.lineBottom = y0src + t1 * ydelta;

    return box;        // (clipped) line is drawn
}

function createRequestObject() {
    if (typeof XMLHttpRequest === 'undefined') {
        XMLHttpRequest = function() {
            try { return new ActiveXObject("Msxml2.XMLHTTP.6.0"); }
            catch(e) {}
            try { return new ActiveXObject("Msxml2.XMLHTTP.3.0"); }
            catch(e) {}
            try { return new ActiveXObject("Msxml2.XMLHTTP"); }
            catch(e) {}
            try { return new ActiveXObject("Microsoft.XMLHTTP"); }
            catch(e) {}
            throw new Error("This browser does not support XMLHttpRequest.");
        };
    }
    return new XMLHttpRequest();
}
