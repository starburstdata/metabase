import React from "react";
import { t } from "ttag";

import type { Collection } from "metabase-types/api";
import Icon from "metabase/components/Icon";
import { DragOverlay } from "./UploadOverlay.styled";

// eslint-disable-next-line import/no-default-export -- deprecated usage
export default function UploadOverlay({
  isDragActive,
  collection,
}: {
  isDragActive: boolean;
  collection: Collection;
}) {
  return (
    <DragOverlay isDragActive={isDragActive}>
      <Icon name="arrow_up" />
      <div>{t`Drop here to upload to ${collection.name}`}</div>
    </DragOverlay>
  );
}
