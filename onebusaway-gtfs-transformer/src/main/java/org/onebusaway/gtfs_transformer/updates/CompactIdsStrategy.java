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

import org.onebusaway.gtfs.model.Agency;
import org.onebusaway.gtfs.model.AgencyAndId;
import org.onebusaway.gtfs.model.IdentityBean;
import org.onebusaway.gtfs.model.ServiceCalendar;
import org.onebusaway.gtfs.model.ServiceCalendarDate;
import org.onebusaway.gtfs.model.ShapePoint;
import org.onebusaway.gtfs.model.Trip;
import org.onebusaway.gtfs.services.GtfsMutableRelationalDao;
import org.onebusaway.gtfs_transformer.services.GtfsTransformStrategy;
import org.onebusaway.gtfs_transformer.services.TransformContext;

import java.util.HashMap;
import java.util.Map;

public class CompactIdsStrategy implements GtfsTransformStrategy {

  @Override
  public void run(TransformContext context, GtfsMutableRelationalDao dao) {
    Map<String, String> agencyIdMapping = renameAgencyIds(dao.getAllAgencies());
    renameIds(dao.getAllRoutes(), "r", agencyIdMapping);
    renameIds(dao.getAllTrips(), "t", agencyIdMapping);
    renameIds(dao.getAllStops(), "s", agencyIdMapping);
    renameServiceIds(dao, agencyIdMapping);
    renameShapeIds(dao, agencyIdMapping);

    UpdateLibrary.reindexDaoIds(dao);
  }

  private Map<String, String> renameAgencyIds(Iterable<Agency> agencies) {
    Map<String, String> agencyIdMapping = new HashMap<String, String>();
    for (Agency agency : agencies) {
      if (agency.getId() == null || agency.getId().isEmpty()) {
        continue;
      }
      agency.setId(mapAgencyId(agencyIdMapping, agency.getId()));
    }
    return agencyIdMapping;
  }

  private <T extends IdentityBean<AgencyAndId>> void renameIds(
      Iterable<T> elements, String prefix,
      Map<String, String> agencyIdMapping) {
    int index = 0;
    for (T element : elements) {
      AgencyAndId id = element.getId();
      AgencyAndId newId = new AgencyAndId(
          mapAgencyId(agencyIdMapping, id.getAgencyId()), prefix + index);
      element.setId(newId);
      index++;
    }
  }

  private void renameServiceIds(
      GtfsMutableRelationalDao dao, Map<String, String> agencyIdMapping) {
    Map<AgencyAndId, AgencyAndId> idMapping = new HashMap<
        AgencyAndId, AgencyAndId>();
    for (ServiceCalendar calendar : dao.getAllCalendars()) {
      calendar.setServiceId(
          mapId(agencyIdMapping, idMapping, calendar.getServiceId(), "c"));
    }
    for (ServiceCalendarDate calendarDate : dao.getAllCalendarDates()) {
      calendarDate.setServiceId(
          mapId(agencyIdMapping, idMapping, calendarDate.getServiceId(), "c"));
    }
    for (Trip trip : dao.getAllTrips()) {
      trip.setServiceId(
          mapId(agencyIdMapping, idMapping, trip.getServiceId(), "c"));
    }
  }

  private void renameShapeIds(
      GtfsMutableRelationalDao dao, Map<String, String> agencyIdMapping) {
    Map<AgencyAndId, AgencyAndId> idMapping = new HashMap<
        AgencyAndId, AgencyAndId>();
    for (ShapePoint shapePoint : dao.getAllShapePoints()) {
      shapePoint.setShapeId(
          mapId(agencyIdMapping, idMapping, shapePoint.getShapeId(), "sh"));
    }
    for (Trip trip : dao.getAllTrips()) {
      trip.setShapeId(
          mapId(agencyIdMapping, idMapping, trip.getShapeId(), "sh"));
    }
  }

  private String mapAgencyId(
      Map<String, String> agencyIdMapping, String oldId) {
    String newId = agencyIdMapping.get(oldId);
    if (newId == null) {
      newId = "a" + agencyIdMapping.size();
      agencyIdMapping.put(oldId, newId);
    }
    return newId;
  }

  private AgencyAndId mapId(Map<String, String> agencyIdMapping,
      Map<AgencyAndId, AgencyAndId> idMapping, AgencyAndId oldId,
      String prefix) {
    if (oldId == null) {
      return null;
    }
    AgencyAndId newId = idMapping.get(oldId);
    if (newId == null) {
      String agencyId = mapAgencyId(agencyIdMapping, oldId.getAgencyId());
      newId = new AgencyAndId(agencyId, prefix + idMapping.size());
      idMapping.put(oldId, newId);
    }
    return newId;
  }
}