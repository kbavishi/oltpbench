/******************************************************************************
 *  Copyright 2015 by OLTPBenchmark Project                                   *
 *                                                                            *
 *  Licensed under the Apache License, Version 2.0 (the "License");           *
 *  you may not use this file except in compliance with the License.          *
 *  You may obtain a copy of the License at                                   *
 *                                                                            *
 *    http://www.apache.org/licenses/LICENSE-2.0                              *
 *                                                                            *
 *  Unless required by applicable law or agreed to in writing, software       *
 *  distributed under the License is distributed on an "AS IS" BASIS,         *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  *
 *  See the License for the specific language governing permissions and       *
 *  limitations under the License.                                            *
 ******************************************************************************/


package com.oltpbenchmark.benchmarks.twitter.procedures;

import java.util.ArrayList;
import java.util.Iterator;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.twitter.TwitterConstants;

public class GetTweetsFromFollowing extends Procedure {

    public final SQLStmt getFollowing = new SQLStmt(
        "SELECT f2 FROM " + TwitterConstants.TABLENAME_FOLLOWS +
        " WHERE f1 = ? LIMIT " + TwitterConstants.LIMIT_FOLLOWERS
    );
    
    /** NOTE: The ?? is substituted into a string of repeated ?'s */
    public final SQLStmt getTweets = new SQLStmt(
        "SELECT * FROM " + TwitterConstants.TABLENAME_TWEETS +
        " WHERE uid IN (??)", TwitterConstants.LIMIT_FOLLOWERS
    );
    
    public void run(Connection conn, int uid, boolean storeResults) throws SQLException {
        PreparedStatement stmt = this.getPreparedStatement(conn, getFollowing);
        stmt.setLong(1, uid);
        ResultSet rs = stmt.executeQuery();

        ArrayList<Long> following = new ArrayList<Long>(TwitterConstants.LIMIT_FOLLOWERS);
        while (rs.next()) {
            following.add(rs.getLong(1));
        }
        
        SQLStmt getTweetsCompact = new SQLStmt(
            "SELECT * FROM " + TwitterConstants.TABLENAME_TWEETS +
            " WHERE uid IN (??)", following.size()
        );
        stmt = this.getPreparedStatement(conn, getTweetsCompact);
        Iterator it = following.iterator();
        int ctr = 0;

        ArrayList<Object> res = new ArrayList<Object>(TwitterConstants.LIMIT_FOLLOWERS);
        while (it.hasNext()) {
            ctr++;
            Long nextElem = (Long) it.next();
            stmt.setLong(ctr, nextElem.longValue());
            if (storeResults) {
                res.add(nextElem);
            }
        } // WHILE
        rs.close();

        if (ctr > 0) {
            if (storeResults) {
                this.results = res;
            }
            rs = stmt.executeQuery();
            rs.close();
        } else {
            this.results = null;
            // LOG.debug("No followers for user: "+uid); // so what .. ?
        }
    }
    
}
