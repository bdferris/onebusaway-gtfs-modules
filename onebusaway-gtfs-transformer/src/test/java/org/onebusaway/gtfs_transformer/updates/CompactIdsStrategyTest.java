/**
 * Copyright (C) 2013 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.onebusaway.gtfs_transformer.updates;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.Before;
import org.junit.Test;
import org.onebusaway.gtfs.model.Agency;
import org.onebusaway.gtfs.model.AgencyAndId;
import org.onebusaway.gtfs.model.IdentityBean;
import org.onebusaway.gtfs.model.ServiceCalendar;
import org.onebusaway.gtfs.model.ShapePoint;
import org.onebusaway.gtfs.model.Trip;
import org.onebusaway.gtfs.services.GtfsMutableRelationalDao;
import org.onebusaway.gtfs.services.MockGtfs;
import org.onebusaway.gtfs_transformer.services.TransformContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class CompactIdsStrategyTest {

  private CompactIdsStrategy _strategy = new CompactIdsStrategy();

  private MockGtfs _gtfs;

  @Before
  public void before() throws IOException {
    _gtfs = MockGtfs.create();
  }

  @Test
  public void test() throws IOException {
    _gtfs.putAgencies(1, "agency_id=A_0");
    _gtfs.putStops(3, "stop_id=S_0,S_1,S_2");
    _gtfs.putRoutes(2, "route_id=R_0,R_1");
    _gtfs.putCalendars(
        1, "start_date=20120903", "end_date=20121016", "mask=1111100");
    _gtfs.putTrips(2, "R_0,R_1", "sid0", "trip_id=T_0,T_1", "shape_id=SHAPE_0");
    _gtfs.putStopTimes("T_0,T_1", "S_0,S_1,S_2");
    _gtfs.putLines("shapes.txt",
        "shape_id,shape_pt_lat,shape_pt_lon,shape_pt_sequence",
        "SHAPE_0,47.0,-122.0,0");

    GtfsMutableRelationalDao dao = _gtfs.read();
    _strategy.run(new TransformContext(), dao);

    Collection<Agency> agencies = dao.getAllAgencies();
    assertEquals(1, agencies.size());
    Agency agency = agencies.iterator().next();
    assertEquals("a0", agency.getId());

    assertEquals(Arrays.asList(id("s0"), id("s1"), id("s2")),
        getAllIds(dao.getAllStops()));
    assertEquals(
        Arrays.asList(id("r0"), id("r1")), getAllIds(dao.getAllRoutes()));
    assertEquals(
        Arrays.asList(id("t0"), id("t1")), getAllIds(dao.getAllTrips()));

    Trip trip = dao.getTripForId(id("t0"));
    assertNotNull(trip);
    assertEquals(id("c0"), trip.getServiceId());
    assertEquals(id("sh0"), trip.getShapeId());

    Collection<ServiceCalendar> calendars = dao.getAllCalendars();
    assertEquals(1, calendars.size());
    ServiceCalendar calendar = calendars.iterator().next();
    assertEquals(id("c0"), calendar.getServiceId());

    Collection<ShapePoint> points = dao.getAllShapePoints();
    assertEquals(1, points.size());
    ShapePoint point = points.iterator().next();
    assertEquals(id("sh0"), point.getShapeId());
  }

  private static AgencyAndId id(String id) {
    return new AgencyAndId("a0", id);
  }

  private <T extends IdentityBean<AgencyAndId>> List<AgencyAndId> getAllIds(
      Iterable<T> elements) {
    List<AgencyAndId> ids = new ArrayList<AgencyAndId>();
    for (T element : elements) {
      ids.add(element.getId());
    }
    Collections.sort(ids);
    return ids;
  }
}
