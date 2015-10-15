package com.ccc.SERPHistogram;

import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Set;

/**
 *
 * @author Tom
 */
public class SERPHistogram {

    private HashMap<Long, HistElement> docs;
    private double currentPeriodStart;
    private double histClicks, curClicks;

    public SERPHistogram(double currentPeriodStart) {
        this.currentPeriodStart = currentPeriodStart;
        docs = new HashMap<Long, HistElement>();
        histClicks = 0;
        curClicks = 0;

    }

    private void evalStats(double epsilon) {
        HistElement el;
        double numHistZeros = numZeroHistEl();
        if (numHistZeros == docs.size()) { // all history docs were zero, we make out the whole histogram...
            epsilon = 1.0 / docs.size();
            // note that normCoef makes no sense in this case, but it wont be used
        }
        double normCoef = 1.0 + numHistZeros * epsilon;
        for (Entry<Long, HistElement> entry : docs.entrySet()) {
            el = entry.getValue();
            // normalize current
            el.relCurClicks = el.curClicks / curClicks;
            // normalize history
            if (el.histClicks == 0) {
                el.relHistClicks = epsilon;
            } else {
                el.relHistClicks = el.histClicks / (histClicks * normCoef);

            }
        }
    }

    private double numZeroHistEl() {
        double nz = 0;
        for (Entry<Long, HistElement> entry : docs.entrySet()) {
            if (entry.getValue().histClicks == 0) {
                nz += 1;
            }
        }
        return nz;
    }

    public double getHistClicks() {
        return histClicks;
    }

    public double getCurClicks() {
        return curClicks;
    }

    public void addToHist(Long urlHash, Double time, Long uidHash) {
        HistElement el = docs.get(urlHash);
        if (el == null) {
            el = new HistElement();
            docs.put(urlHash, el);
        }
        if (time < currentPeriodStart) {
            el.incerementHist();
            histClicks += 1;
        } else {
            el.incerementCur(uidHash);
            curClicks += 1;
        }

    }

    public double kld(double epsilon) {
        if (curClicks == 0) {
            return -1;
        }
        evalStats(epsilon);
        HistElement el;
        double kld = 0.0;
        for (Entry<Long, HistElement> entry : docs.entrySet()) {
            el = entry.getValue();
            kld += el.relCurClicks * Math.max(Math.log(el.relCurClicks / el.relHistClicks), 0);
        }
        return kld + Math.log(curClicks / (1 + histClicks));

    }

    public Set<Entry<Long, HistElement>> entrySet() {
        return docs.entrySet();
    }

    public int numDocs() {
        return docs.size();
    }
}
