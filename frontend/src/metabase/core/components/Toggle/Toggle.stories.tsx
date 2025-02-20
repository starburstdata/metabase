import React from "react";
import type { ComponentStory } from "@storybook/react";
import { useArgs } from "@storybook/client-api";
import Toggle from "./Toggle";

// eslint-disable-next-line import/no-default-export -- deprecated usage
export default {
  title: "Core/Toggle",
  component: Toggle,
};

const Template: ComponentStory<typeof Toggle> = args => {
  const [{ value }, updateArgs] = useArgs();
  const handleChange = (value: boolean) => updateArgs({ value });

  return <Toggle {...args} value={value} onChange={handleChange} />;
};

export const Default = Template.bind({});
Default.args = {
  value: false,
};
