package com.ccc.SERPHistogram;

import java.util.HashSet;

/**
 *
 * @author Tom
 */
    public class HistElement {

        public double histClicks, curClicks,relCurClicks,relHistClicks; // history and current clicks
        public HashSet<Long> users; // Set of users from current period
        public HistElement() {
            users = new HashSet<Long>();
            histClicks = 0.0;
            curClicks = 0.0;
            relCurClicks=0.0;
            relHistClicks=0.0;
        }

        public void incerementHist() {
            histClicks += 1;
        }

        public void incerementCur(Long user) {
            curClicks += 1;
            users.add(user);
        }

    }
