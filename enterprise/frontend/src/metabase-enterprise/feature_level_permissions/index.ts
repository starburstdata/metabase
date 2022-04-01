import { hasPremiumFeature } from "metabase-enterprise/settings";
import { PLUGIN_FEATURE_LEVEL_PERMISSIONS } from "metabase/plugins";
import { DATA_PERMISSIONS_TOOLBAR_CONTENT } from "metabase/admin/permissions/pages/DataPermissionsPage/DataPermissionsPage.jsx";
import {
  canAccessSettings,
  canAccessDataModel,
  canAccessDatabaseManagement,
} from "./utils";

import { getFeatureLevelDataPermissions } from "./permissions";
import { DATA_COLUMNS } from "./constants";

if (hasPremiumFeature("advanced_permissions")) {
  PLUGIN_FEATURE_LEVEL_PERMISSIONS.canAccessSettings = canAccessSettings;
  PLUGIN_FEATURE_LEVEL_PERMISSIONS.canAccessDataModel = canAccessDataModel;
  PLUGIN_FEATURE_LEVEL_PERMISSIONS.canAccessDatabaseManagement = canAccessDatabaseManagement;

  PLUGIN_FEATURE_LEVEL_PERMISSIONS.getFeatureLevelDataPermissions = getFeatureLevelDataPermissions;
  PLUGIN_FEATURE_LEVEL_PERMISSIONS.dataColumns = DATA_COLUMNS;
  DATA_PERMISSIONS_TOOLBAR_CONTENT.length = 0;
}
